package org.folio.oaipmh.processors;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.NEXT_RECORD_ID_PARAM;
import static org.folio.oaipmh.Constants.OFFSET_PARAM;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.REQUEST_ID_PARAM;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.ws.rs.core.Response;

import org.apache.commons.collections4.CollectionUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.AbstractHelper;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.helpers.streaming.BatchStreamWrapper;
import org.folio.rest.client.SourceStorageSourceRecordsClient;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.SQLConnection;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.StatusType;
import org.springframework.util.ReflectionUtils;

import com.google.common.collect.Maps;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.parsetools.impl.JsonParserImpl;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.impl.Connection;


public class MarcWithHoldingsRequestHelper extends AbstractHelper {

  private static final int DATABASE_FETCHING_CHUNK_SIZE = 50;

  private static final String INSTANCES_TABLE_NAME = "INSTANCES";

  private static final String INSTANCE_ID_COLUMN_NAME = "INSTANCE_ID";

  private static final String REQUEST_ID_COLUMN_NAME = "REQUEST_ID";

  private static final String INSTANCE_ID_FIELD_NAME = "instanceId";

  private static final String SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS = "skipSuppressedFromDiscoveryRecords";

  private static final String INSTANCE_IDS_ENRICH_PARAM_NAME = "instanceIds";

  private static final String DELETED_RECORD_SUPPORT_PARAM_NAME = "deletedRecordSupport";

  private static final String START_DATE_PARAM_NAME = "startDate";

  private static final String END_DATE_PARAM_NAME = "endDate";

  private static final String INVENTORY_INSTANCES_ENDPOINT = "/oai-pmh-view/enrichedInstances";

  private static final String INVENTORY_UPDATED_INSTANCES_ENDPOINT = "/inventory-hierarchy/updated-instance-ids";

  private static final int REQUEST_TIMEOUT = 604800000;
  private static final String ERROR_FROM_STORAGE = "Got error response from %s, message: %s";

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public static final MarcWithHoldingsRequestHelper INSTANCE = new MarcWithHoldingsRequestHelper();

  /**
   * The dates returned by inventory storage service are in format "2018-09-19T02:52:08.873+0000".
   * Using {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME} and just in case 2 offsets "+HHmm" and "+HH:MM"
   */
  private static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    .optionalStart().appendOffset("+HH:MM", "Z").optionalEnd()
    .optionalStart().appendOffset("+HHmm", "Z").optionalEnd()
    .toFormatter();

  public static MarcWithHoldingsRequestHelper getInstance() {
    return INSTANCE;
  }

  /**
   * Handle MarcWithHoldings request
   */
  @Override
  public Future<Response> handle(Request request, Context vertxContext) {
    Promise<Response> promise = Promise.promise();
    try {
      PostgresClient postgresClient = PostgresClient.getInstance(vertxContext.owner(), request.getTenant());
      String resumptionToken = request.getResumptionToken();
      final boolean deletedRecordSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request);
      List<OAIPMHerrorType> errors = validateListRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, promise, errors);
      }

      String requestId =
        (resumptionToken == null || request.getRequestId() == null) ? UUID
          .randomUUID().toString() : request.getRequestId();
      Promise<Void> fetchingIdsPromise;
      if (resumptionToken == null
        || request.getRequestId() == null) { // the first request from EDS
        fetchingIdsPromise = createBatchStream(request, promise, vertxContext, requestId, postgresClient);
        fetchingIdsPromise.future().onComplete(e -> processBatch(request, vertxContext, postgresClient, promise, deletedRecordSupport, requestId, true));
      } else {
        processBatch(request, vertxContext, postgresClient, promise, deletedRecordSupport, requestId, false);
      }
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  private void processBatch(Request request, Context context, PostgresClient postgresClient, Promise<Response> oaiPmhResponsePromise, boolean deletedRecordSupport, String requestId, boolean firstBatch) {
    try {

      int batchSize = Integer.parseInt(
        RepositoryConfigurationUtil.getProperty(request.getTenant(),
          REPOSITORY_MAX_RECORDS_PER_RESPONSE));

      getNextInstances(request, requestId, batchSize, context, postgresClient).future().onComplete(fut -> {
        if (fut.failed()) {
          logger.error("Get instances failed: " + fut.cause());
          oaiPmhResponsePromise.fail(fut.cause());
          return;
        }
        List<JsonObject> instances = fut.result();
        logger.info("Processing instances: " + instances.size());
        if (CollectionUtils.isEmpty(instances) && !firstBatch) { // resumption token doesn't exist in context
          handleException(oaiPmhResponsePromise, new IllegalArgumentException(
            "Specified resumption token doesn't exists"));
          return;
        }

        if (!instances.isEmpty() && instances.get(0).getString(INSTANCE_ID_FIELD_NAME).equals(request.getNextRecordId())) {
          handleException(oaiPmhResponsePromise, new IllegalArgumentException(
            "Stale resumption token"));
          return;
        }

        if (CollectionUtils.isEmpty(instances)) {
          buildRecordsResponse(request, requestId, instances, new HashMap<>(),
            firstBatch, null, deletedRecordSupport)
            .onSuccess(e -> postgresClient.closeClient(o -> oaiPmhResponsePromise.complete(e)))
            .onFailure(e -> handleException(oaiPmhResponsePromise, e));
          return;
        }

        String nextInstanceId = instances.size() < batchSize ? null : instances.get(batchSize).getString(INSTANCE_ID_FIELD_NAME);
        List<JsonObject> instancesWithoutLast = nextInstanceId != null ? instances.subList(0, batchSize) : instances;
        final SourceStorageSourceRecordsClient srsClient = new SourceStorageSourceRecordsClient(request.getOkapiUrl(),
          request.getTenant(), request.getOkapiToken());


        Future<Map<String, JsonObject>> srsResponse = Future.future();
        if (CollectionUtils.isNotEmpty(instances)) {
          srsResponse = requestSRSByIdentifiers(srsClient, instancesWithoutLast, deletedRecordSupport);
        } else {
          srsResponse.complete();
        }
        srsResponse.onSuccess(res -> buildRecordsResponse(request, requestId, instancesWithoutLast, res,
          firstBatch, nextInstanceId, deletedRecordSupport).onSuccess(result -> {
          deleteInstanceIds(instancesWithoutLast.stream()
            .map(e -> e.getString(INSTANCE_ID_FIELD_NAME))
            .collect(toList()), requestId, postgresClient)
            .future().onComplete(e -> postgresClient.closeClient(o -> oaiPmhResponsePromise.complete(result)));
        }).onFailure(e -> handleException(oaiPmhResponsePromise, e)));
        srsResponse.onFailure(t -> handleException(oaiPmhResponsePromise, t));
      });
    } catch (Exception e) {
      handleException(oaiPmhResponsePromise, e);
    }
  }

  private Promise<Void> deleteInstanceIds(List<String> instanceIds, String requestId, PostgresClient postgresClient) {
    Promise<Void> promise = Promise.promise();
    String instanceIdsStr = instanceIds.stream().map(e -> "'" + e + "'").collect(Collectors.joining(", "));
    final String sql = format("DELETE FROM " + INSTANCES_TABLE_NAME + " WHERE " +
      REQUEST_ID_COLUMN_NAME + " = '%s' AND " + INSTANCE_ID_COLUMN_NAME + " IN (%s)", requestId, instanceIdsStr);
    postgresClient.startTx(conn -> {
      try {
        postgresClient.execute(conn, sql, reply -> {
          if (reply.succeeded()) {
            endTransaction(postgresClient, conn).future().onComplete(o -> promise.complete());
          } else {
            endTransaction(postgresClient, conn).future().onComplete(o -> handleException(promise, reply.cause()));
          }
        });
      } catch (Exception e) {
        endTransaction(postgresClient, conn).future().onComplete(o -> handleException(promise, e));
      }
    });
    return promise;
  }

  private Promise<List<JsonObject>> getNextInstances(Request request, String requestId, int batchSize, Context context, PostgresClient postgresClient) {
    Promise<List<JsonObject>> promise = Promise.promise();
    final String sql = format("SELECT json FROM " + INSTANCES_TABLE_NAME + " WHERE " +
      REQUEST_ID_COLUMN_NAME + " = '%s' ORDER BY " + INSTANCE_ID_COLUMN_NAME + " LIMIT %d", requestId, batchSize + 1);
    logger.info("selecting instances");
    postgresClient.startTx(conn -> {
      if (conn.failed()) {
        logger.error("Cannot get connection for saving ids: " + conn.cause().getMessage(), conn.cause());
      } else {
        try {
          postgresClient.select(conn, sql, reply -> {
            if (reply.succeeded()) {
              logger.info("Instances select result: " + reply.result());
              List<JsonObject> list = StreamSupport
                .stream(reply.result().spliterator(), false)
                .map(this::createJsonFromRow).map(e -> e.getJsonObject("json")).collect(toList());
              enrichInstances(list, request, context)
                .future().onComplete(enrichInstancesResult ->
                    endTransaction(postgresClient, conn).future().onComplete(o -> {
                      if(enrichInstancesResult.succeeded()) {
                        promise.complete(enrichInstancesResult.result());
                      } else {
                        promise.fail(enrichInstancesResult.cause());
                      }
                    })
                  );
            } else {
              logger.error("Selecting instances failed: " + reply.cause());
              endTransaction(postgresClient, conn).future().onComplete(o -> handleException(promise, reply.cause()));
            }
          });
        } catch (Exception e) {
          endTransaction(postgresClient, conn).future().onComplete(o -> handleException(promise, e));
        }
      }
    });
    return promise;
  }

  private Promise<Void> endTransaction(PostgresClient postgresClient, AsyncResult<SQLConnection> conn) {
    Promise<Void> promise = Promise.promise();
    try {
      postgresClient.endTx(conn, promise);
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise;
  }


  private Promise<List<JsonObject>> enrichInstances(List<JsonObject> result, Request request, Context context) {
    Map<String, JsonObject> instances = result.stream().collect(toMap(e -> e.getString(INSTANCE_ID_FIELD_NAME), Function.identity()));
    Promise<List<JsonObject>> completePromise = Promise.promise();
    HttpClient httpClient = context.owner().createHttpClient();

    HttpClientRequest enrichInventoryClientRequest = createEnrichInventoryClientRequest(httpClient, request);
    BatchStreamWrapper databaseWriteStream = getBatchHttpStream(httpClient, completePromise, enrichInventoryClientRequest, context);
    JsonObject entries = new JsonObject();
    entries.put(INSTANCE_IDS_ENRICH_PARAM_NAME, new JsonArray(new ArrayList<>(instances.keySet())));
    entries.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS, isSkipSuppressed(request));
    enrichInventoryClientRequest.end(entries.encode());

    databaseWriteStream.handleBatch(batch -> {
      try {
        for (JsonEvent jsonEvent : batch) {
          JsonObject value = jsonEvent.objectValue();
          String instanceId = value.getString(INSTANCE_ID_FIELD_NAME);
          Object itemsandholdingsfields = value.getValue(RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS);
          if (itemsandholdingsfields instanceof JsonObject) {
            JsonObject instance = instances.get(instanceId);
            if (instance != null) {
              enrichDiscoverySuppressed((JsonObject) itemsandholdingsfields, instance);
              instance.put(RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS,
                itemsandholdingsfields);
            } else { // it can be the case only for testing
              logger.info(format("Instance with instanceId %s wasn't in the request", instanceId));
            }
          }
        }

        if (databaseWriteStream.isTheLastBatch() && !completePromise.future().isComplete()) {
          completePromise.complete(new ArrayList<>(instances.values()));
        }
      } catch (Exception e) {
        completePromise.fail(e);
      }
    });

    return completePromise;
  }

  private void enrichDiscoverySuppressed(JsonObject itemsandholdingsfields, JsonObject instance) {
    if (Boolean.parseBoolean(instance.getString("suppressFromDiscovery")))
      for (Object item : itemsandholdingsfields.getJsonArray("items")) {
        if (item instanceof JsonObject) {
          JsonObject itemJson = (JsonObject) item;
          itemJson.put(RecordMetadataManager.INVENTORY_SUPPRESS_DISCOVERY_FIELD, true);
        }
      }
  }


  private JsonObject createJsonFromRow(Row row) {
    JsonObject json = new JsonObject();
    if (row != null) {
      for (int i = 0; i < row.size(); i++) {
        json.put(row.getColumnName(i), convertRowValue(row.getValue(i)));
      }
    }
    return json;
  }

  private Object convertRowValue(Object value) {
    if (value == null) {
      return "";
    }
    return value instanceof JsonObject ? value : value.toString();
  }

  private ResumptionTokenType buildResumptionTokenFromRequest(Request request, String id, long returnedCount, String nextInstanceId) {
    long offset = returnedCount + request.getOffset();
    if (nextInstanceId == null) {
      return new ResumptionTokenType()
        .withValue("")
        .withCursor(
          BigInteger.valueOf(offset));
    }
    Map<String, String> extraParams = new HashMap<>();
    extraParams.put(OFFSET_PARAM, String.valueOf(returnedCount));
    extraParams.put(REQUEST_ID_PARAM, id);
    extraParams.put(NEXT_RECORD_ID_PARAM, nextInstanceId);
    if (request.getUntil() == null) {
      extraParams.put(UNTIL_PARAM, getUntilDate(request, request.getFrom()));
    }

    String resumptionToken = request.toResumptionToken(extraParams);

    return new ResumptionTokenType()
      .withValue(resumptionToken)
      .withCursor(
        request.getOffset() == 0 ? BigInteger.ZERO : BigInteger.valueOf(request.getOffset()));
  }

  private Future<Response> buildRecordsResponse(
    Request request, String requestId, List<JsonObject> batch,
    Map<String, JsonObject> srsResponse, boolean firstBatch,
    String nextInstanceId, boolean deletedRecordSupport) {

    Promise<Response> promise = Promise.promise();
    try {
      logger.info("Build records response: " + batch.size());
      List<RecordType> records = buildRecordsList(request, batch, srsResponse, deletedRecordSupport);

      logger.info("Build records response: " + records.size());
      ResponseHelper responseHelper = getResponseHelper();
      OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request);
      if (records.isEmpty() && nextInstanceId == null && (firstBatch && batch.isEmpty())) {
        oaipmh.withErrors(createNoRecordsFoundError());
      } else {
        oaipmh.withListRecords(new ListRecordsType().withRecords(records));
      }
      Response response;
      if (oaipmh.getErrors().isEmpty()) {
        if (!firstBatch || nextInstanceId != null) {
          ResumptionTokenType resumptionToken = buildResumptionTokenFromRequest(request, requestId,
            records.size(), nextInstanceId);
          oaipmh.getListRecords().withResumptionToken(resumptionToken);
        }
        response = responseHelper.buildSuccessResponse(oaipmh);
      } else {
        response = responseHelper.buildFailureResponse(oaipmh, request);
      }

      promise.complete(response);
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  private List<RecordType> buildRecordsList(Request request, List<JsonObject> batch, Map<String, JsonObject> srsResponse,
                                            boolean deletedRecordSupport) {
    RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();

    final boolean suppressedRecordsProcessing = getBooleanProperty(request.getOkapiHeaders(),
      REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

    List<RecordType> records = new ArrayList<>();

    for (JsonObject inventoryInstance : batch) {
      final String instanceId = inventoryInstance.getString(INSTANCE_ID_FIELD_NAME);
      final JsonObject srsInstance = srsResponse.get(instanceId);
      if (srsInstance == null) {
        continue;
      }
      JsonObject updatedSrsInstance = metadataManager
        .populateMetadataWithItemsData(srsInstance, inventoryInstance,
          suppressedRecordsProcessing);
      String identifierPrefix = request.getIdentifierPrefix();
      RecordType record = new RecordType()
        .withHeader(createHeader(inventoryInstance)
          .withIdentifier(getIdentifier(identifierPrefix, instanceId)));

      if (deletedRecordSupport && storageHelper.isRecordMarkAsDeleted(updatedSrsInstance)) {
        record.getHeader().setStatus(StatusType.DELETED);
      }
      String source = storageHelper.getInstanceRecordSource(updatedSrsInstance);
      if (source != null && record.getHeader().getStatus() == null) {
        if (suppressedRecordsProcessing) {
          source = metadataManager.updateMetadataSourceWithDiscoverySuppressedData(source, updatedSrsInstance);
          source = metadataManager.updateElectronicAccessFieldWithDiscoverySuppressedData(source, updatedSrsInstance);
        }
        record.withMetadata(buildOaiMetadata(request, source));
      }

      if (filterInstance(request, srsInstance)) {
        records.add(record);
      }
    }
    return records;
  }

  @Override
  protected HeaderType createHeader(JsonObject instance) {
    String updatedDate = instance.getString("updatedDate");
    Instant datetime = formatter.parse(updatedDate, Instant::from)
      .truncatedTo(ChronoUnit.SECONDS);

    return new HeaderType()
      .withDatestamp(datetime)
      .withSetSpecs("all");
  }

  private HttpClientRequest buildInventoryQuery(HttpClient httpClient, Request request) {
    Map<String, String> paramMap = new HashMap<>();
    Date date = convertStringToDate(request.getFrom(), false);
    if (date != null) {
      paramMap.put(START_DATE_PARAM_NAME, dateFormat.format(date));
    }
    date = convertStringToDate(request.getUntil(), true);
    if (date != null) {
      paramMap.put(END_DATE_PARAM_NAME, dateFormat.format(date));
    }
    paramMap.put(DELETED_RECORD_SUPPORT_PARAM_NAME,
      String.valueOf(
        RepositoryConfigurationUtil.isDeletedRecordsEnabled(request)));
    paramMap.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS,
      String.valueOf(
        isSkipSuppressed(request)));

    final String params = paramMap.entrySet().stream()
      .map(e -> e.getKey() + "=" + e.getValue())
      .collect(Collectors.joining("&"));

    String inventoryQuery = format("%s%s?%s", request.getOkapiUrl(), INVENTORY_UPDATED_INSTANCES_ENDPOINT, params);


    logger.info("Sending request to : " + inventoryQuery);
    final HttpClientRequest httpClientRequest = httpClient
      .getAbs(inventoryQuery);

    httpClientRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpClientRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpClientRequest.putHeader(ACCEPT, APPLICATION_JSON);

    httpClientRequest.setTimeout(REQUEST_TIMEOUT);

    return httpClientRequest;
  }

  private boolean isSkipSuppressed(Request request) {
    return !getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
  }

  private Promise<Void> createBatchStream(Request request,
                                          Promise<Response> oaiPmhResponsePromise,
                                          Context vertxContext, String requestId, PostgresClient postgresClient) {
    Promise<Void> completePromise = Promise.promise();
    final HttpClientOptions options = new HttpClientOptions();
    options.setKeepAliveTimeout(REQUEST_TIMEOUT);
    options.setConnectTimeout(REQUEST_TIMEOUT);
    options.setIdleTimeout(REQUEST_TIMEOUT);
    HttpClient httpClient = vertxContext.owner().createHttpClient(options);
    HttpClientRequest httpClientRequest = buildInventoryQuery(httpClient, request);
    BatchStreamWrapper databaseWriteStream = getBatchHttpStream(httpClient, oaiPmhResponsePromise, httpClientRequest, vertxContext);
    httpClientRequest.sendHead();

    ArrayDeque<Promise<Connection>> queue = (ArrayDeque<Promise<Connection>>)
      getValueFrom(getValueFrom(getValueFrom(postgresClient, "client"), "pool"),
        "waiters");

    databaseWriteStream.setCapacityChecker(()-> queue.size() > 20);

    databaseWriteStream.handleBatch(batch -> {
      Promise<Void> savePromise = saveInstancesIds(batch, request, requestId, postgresClient, databaseWriteStream);
      final Long returnedCount = databaseWriteStream.getReturnedCount();

      if (returnedCount % 1000 == 0) {
        logger.info("Batch saving progress: " + returnedCount + " returned so far, batch size: " + batch.size() + ", http ended: " + databaseWriteStream.isStreamEnded());
      }

      if (databaseWriteStream.isTheLastBatch()) {
        savePromise.future().toCompletionStage().thenRun(completePromise::complete);
      }
      databaseWriteStream.invokeDrainHandler();
    });

    return completePromise;
  }

  private Object getValueFrom(Object obj, String fieldName) {
    try{
      Field field = requireNonNull(ReflectionUtils.findField(requireNonNull(obj.getClass()), fieldName));
      ReflectionUtils.makeAccessible(field);
      return ReflectionUtils.getField(field, obj);
    } catch (NullPointerException ex){
      logger.error("Cannot get the pool size. Object is null.");
      throw new IllegalArgumentException("Cannot get the pool size. Object is null.");
    }
  }

  private BatchStreamWrapper getBatchHttpStream(HttpClient inventoryHttpClient, Promise<?> promise, HttpClientRequest inventoryQuery, Context vertxContext) {
    final Vertx vertx = vertxContext.owner();

    BatchStreamWrapper databaseWriteStream = new BatchStreamWrapper(vertx, DATABASE_FETCHING_CHUNK_SIZE);

    inventoryQuery.handler(resp -> {
      if(resp.statusCode() != 200) {
        String errorMsg = getErrorFromStorageMessage("inventory-storage", resp.statusMessage());
        logger.error(errorMsg);
        promise.fail(new IllegalStateException(errorMsg));
      } else {
        JsonParser jp = new JsonParserImpl(resp);
        jp.objectValueMode();
        jp.pipeTo(databaseWriteStream);
        jp.endHandler(e -> {
          databaseWriteStream.end();
          inventoryHttpClient.close();
        });
      }
    });

    inventoryQuery.exceptionHandler(e -> {
      logger.error(e.getMessage(), e);
      handleException(promise, e);
    });

    databaseWriteStream.exceptionHandler(e -> {
      if (e != null) {
        handleException(promise, e);
      }
    });
    return databaseWriteStream;
  }

  private Promise<Void> saveInstancesIds(List<JsonEvent> instances, Request request, String requestId, PostgresClient postgresClient, BatchStreamWrapper databaseWriteStream) {
    Promise<Void> promise = Promise.promise();
    postgresClient.getConnection(e -> {
      List<Tuple> batch = new ArrayList<>();
      List<JsonObject> entities = instances.stream().map(JsonEvent::objectValue).collect(toList());

      for (JsonObject jsonObject : entities) {
        String id = jsonObject.getString(INSTANCE_ID_FIELD_NAME);
        batch.add(Tuple.of(UUID.fromString(id), requestId, jsonObject));
      }

      String tenantId = TenantTool.tenantId(request.getOkapiHeaders());
      String sql = "INSERT INTO " + PostgresClient.convertToPsqlStandard(tenantId) + "." + INSTANCES_TABLE_NAME + " (instance_id, request_id, json) VALUES ($1, $2, $3) RETURNING instance_id";

      if (e.failed()) {
        logger.error("Cannot get connection for saving ids: " + e.cause().getMessage(), e.cause());
        promise.fail(e.cause());
      } else {
        PgConnection connection = e.result();
        connection.preparedQuery(sql).executeBatch(batch, queryRes -> {
          if (queryRes.failed()) {
            logger.error("Save instance Ids failed: " + e.cause().getMessage(), e.cause());
            promise.fail(queryRes.cause());
          } else {
            logger.info("Save instance complete");
            promise.complete();
          }
          connection.close();
          databaseWriteStream.invokeDrainHandler();
        });
      }
    });
    return promise;
  }

  private HttpClientRequest createEnrichInventoryClientRequest(HttpClient httpClient, Request request) {
    final HttpClientRequest httpClientRequest = httpClient
      .postAbs(format("%s%s", request.getOkapiUrl(), INVENTORY_INSTANCES_ENDPOINT));

    httpClientRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpClientRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpClientRequest.putHeader(ACCEPT, APPLICATION_JSON);
    httpClientRequest.putHeader(CONTENT_TYPE, APPLICATION_JSON);

    return httpClientRequest;
  }

  private Future<Map<String, JsonObject>> requestSRSByIdentifiers(SourceStorageSourceRecordsClient srsClient,
                                                                  List<JsonObject> batch, boolean deletedRecordSupport) {
    final List<String> listOfIds = extractListOfIdsForSRSRequest(batch);
    logger.info("Request to SRS: {0}", listOfIds);
    Promise<Map<String, JsonObject>> promise = Promise.promise();
    try {
      final Map<String, JsonObject> result = Maps.newHashMap();
      srsClient.postSourceStorageSourceRecords("INSTANCE", deletedRecordSupport, listOfIds, srsResp -> srsResp.bodyHandler(bh -> {
        if(srsResp.statusCode() != 200) {
          String errorMsg = getErrorFromStorageMessage("source-record-storage", srsResp.statusMessage());
          logger.error(errorMsg);
          promise.fail(new IllegalStateException(errorMsg));
        }
          try {
            final Object o = bh.toJson();
            if (o instanceof JsonObject) {
              JsonObject entries = (JsonObject) o;
              final JsonArray records = entries.getJsonArray("sourceRecords");
              records.stream()
                .filter(Objects::nonNull)
                .map(r -> (JsonObject) r)
                .forEach(jo -> result.put(jo.getJsonObject("externalIdsHolder").getString("instanceId"), jo));
            } else {
              logger.debug("Can't process response from SRS: {0}", bh.toString());
            }
            promise.complete(result);
          } catch (Exception e) {
            handleException(promise, e);
          }
        }
      ));
    } catch (Exception e) {
      handleException(promise, e);
    }

    return promise.future();
  }

  private String getErrorFromStorageMessage(String errorSource, String responseMessage) {
    return format(ERROR_FROM_STORAGE, errorSource, responseMessage);
  }

  private List<String> extractListOfIdsForSRSRequest(List<JsonObject> batch) {

    return batch.stream().
      filter(Objects::nonNull)
      .map(instance -> instance.getString(INSTANCE_ID_FIELD_NAME))
      .collect(toList());
  }


  private void handleException(Promise<?> promise, Throwable e) {
    logger.error(e.getMessage(), e);
    promise.fail(e);
  }
}
