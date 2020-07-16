package org.folio.oaipmh.processors;

import java.math.BigInteger;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
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
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.StatusType;

import com.google.common.collect.Maps;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
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
import static java.util.stream.Collectors.toList;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
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


public class MarcWithHoldingsRequestHelper extends AbstractHelper {

  private static final int DATABASE_FETCHING_CHUNK_SIZE = 100;

  private static final String INSTANCES_TABLE_NAME = "INSTANCES";

  private static final String INSTANCE_ID_COLUMN_NAME = "INSTANCE_ID";

  private static final String REQUEST_ID_COLUMN_NAME = "REQUEST_ID";


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

  @Override
  public Future<Response> handle(Request request, Context vertxContext) {
    Promise<Response> promise = Promise.promise();
    try {

      String resumptionToken = request.getResumptionToken();
      final boolean deletedRecordSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request);
      List<OAIPMHerrorType> errors = validateListRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, promise, errors);
      }

      String requestId =
        (resumptionToken == null || request.getNextRecordId() == null) ? UUID
          .randomUUID().toString() : request.getNextRecordId();
      Promise<Void> fetchingIdsPromise;
      if (resumptionToken == null
        || request.getNextRecordId() == null) { // the first request from EDS
        fetchingIdsPromise = createBatchStream(request, promise, vertxContext, requestId);
        fetchingIdsPromise.future().onComplete(e -> processBatch(request, vertxContext, promise, deletedRecordSupport, requestId, true));
      } else {
        processBatch(request, vertxContext, promise, deletedRecordSupport, requestId, false);
      }
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  private void processBatch(Request request, Context context, Promise<Response> promise, boolean deletedRecordSupport, String requestId, boolean firstBatch) {
    try {

      int batchSize = Integer.parseInt(
        RepositoryConfigurationUtil.getProperty(request.getTenant(),
          REPOSITORY_MAX_RECORDS_PER_RESPONSE));

      getNextInstances(requestId, batchSize, context).future().onComplete(fut -> {
        List<JsonObject> instances = fut.result();
        if (CollectionUtils.isEmpty(instances) && !firstBatch) { // resumption token doesn't exist in context
          handleException(promise, new IllegalArgumentException(
            "Specified resumption token doesn't exists"));
          return;
        }

        String nextInstanceId = instances.size() < batchSize ? null : instances.get(batchSize).getString("instanceId");
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
            .map(e -> e.getString("id"))
            .collect(toList()), requestId, context)
            .future().onComplete(e -> promise.complete(result));
          ;
        }));
        srsResponse.onFailure(t -> handleException(promise, t));
      });
    } catch (Exception e) {
      handleException(promise, e);
    }
  }

  private Promise<Void> deleteInstanceIds(List<String> instanceIds, String requestId, Context context) {
    Promise<Void> promise = Promise.promise();
    PostgresClient postgresClient = PostgresClient.getInstance(context.owner());
    final String sql = String.format("DELETE FROM " + INSTANCES_TABLE_NAME + " WHERE " +
      REQUEST_ID_COLUMN_NAME + " = %s AND " + INSTANCE_ID_COLUMN_NAME + " IN (%s)", requestId, String.join(", ", instanceIds));
    postgresClient.startTx(conn -> {
      try {
        postgresClient.execute(conn, sql, reply -> {
          if (reply.succeeded()) {
            promise.complete();
          } else {
            promise.fail(reply.cause());
          }
        });
      } catch (Exception e) {
        handleException(promise, e);
      }
    });
    return promise;
  }

  private Promise<List<JsonObject>> getNextInstances(String requestId, int batchSize, Context context) {
    Promise<List<JsonObject>> promise = Promise.promise();
    PostgresClient postgresClient = PostgresClient.getInstance(context.owner());
    final String sql = String.format("SELECT jsonb FROM " + INSTANCES_TABLE_NAME + " WHERE " +
      REQUEST_ID_COLUMN_NAME + " = %s ORDER BY " + INSTANCE_ID_COLUMN_NAME + " LIMIT %d", requestId, batchSize + 1);
    postgresClient.startTx(conn -> {
      try {
        postgresClient.select(conn, sql, reply -> {
          if (reply.succeeded()) {
            enrichInstances(StreamSupport
              .stream(reply.result().spliterator(), false)
              .map(this::createJsonFromRow).collect(toList()))
              .future().onComplete(e -> promise.complete(e.result()));
            ;
          }
        });
      } catch (Exception e) {
        handleException(promise, e);
      }
    });
    return promise;
  }

  private Promise<List<JsonObject>> enrichInstances(List<JsonObject> result) {

/*    Promise<Void> completePromise = Promise.promise();
    BatchStreamWrapper databaseWriteStream = getBatchHttpStream(request, oaiPmhResponsePromise, vertxContext);

    databaseWriteStream.handleBatch(batch -> {

    });*/

    // List<String> instanceIds = result.stream().map(e -> e.getString("instanceId")).collect(toList());
    return Promise.promise();
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
      List<RecordType> records = buildRecordsList(request, batch, srsResponse, deletedRecordSupport);

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
      final String instanceId = inventoryInstance.getString("instanceid");
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
    String instanceUpdatedDate = instance.getString("instanceupdateddate");
    Instant datetime = formatter.parse(instanceUpdatedDate, Instant::from)
      .truncatedTo(ChronoUnit.SECONDS);

    return new HeaderType()
      .withDatestamp(datetime)
      .withSetSpecs("all");
  }

  private String buildInventoryQuery(Request request) {
    final String inventoryEndpoint = "/oai-pmh-view/instances";
    Map<String, String> paramMap = new HashMap<>();
    Date date = convertStringToDate(request.getFrom(), false);
    if (date != null) {
      paramMap.put("startDate", dateFormat.format(date));
    }
    date = convertStringToDate(request.getUntil(), true);
    if (date != null) {
      paramMap.put("endDate", dateFormat.format(date));
    }
    paramMap.put("deletedRecordSupport",
      String.valueOf(
        RepositoryConfigurationUtil.isDeletedRecordsEnabled(request)));
    paramMap.put("skipSuppressedFromDiscoveryRecords",
      String.valueOf(
        !getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING)));

    final String params = paramMap.entrySet().stream()
      .map(e -> e.getKey() + "=" + e.getValue())
      .collect(Collectors.joining("&"));


    return String.format("%s%s?%s", request.getOkapiUrl(), inventoryEndpoint, params);
  }

  private Promise<Void> createBatchStream(Request request,
                                          Promise<Response> oaiPmhResponsePromise,
                                          Context vertxContext, String requestId) {
    Promise<Void> completePromise = Promise.promise();
    BatchStreamWrapper databaseWriteStream = getBatchHttpStream(request, oaiPmhResponsePromise, vertxContext);

    databaseWriteStream.handleBatch(batch -> {

      saveInstancesIds(batch, requestId, vertxContext);

      boolean theLastBatch = batch.size() < DATABASE_FETCHING_CHUNK_SIZE ||
        (databaseWriteStream.isStreamEnded()
          && databaseWriteStream.getItemsInQueueCount() <= DATABASE_FETCHING_CHUNK_SIZE); //TODO: think about the last condition in terms of new approach
      if (theLastBatch) {
        completePromise.complete();
      }
    });
    return completePromise;
  }

  private BatchStreamWrapper getBatchHttpStream(Request request, Promise<Response> oaiPmhResponsePromise, Context vertxContext) {
    final Vertx vertx = vertxContext.owner();

    BatchStreamWrapper databaseWriteStream = new BatchStreamWrapper(vertx, DATABASE_FETCHING_CHUNK_SIZE);

    final HttpClient inventoryHttpClient = vertx.createHttpClient();
    final HttpClientRequest httpClientRequest = createInventoryClientRequest(inventoryHttpClient, buildInventoryQuery(request),
      request);

    httpClientRequest.handler(resp -> {
      JsonParser jp = new JsonParserImpl(resp);
      jp.objectValueMode();
      jp.pipeTo(databaseWriteStream);
      jp.endHandler(e -> {
        databaseWriteStream.end();
        inventoryHttpClient.close();
      });
    });

    httpClientRequest.exceptionHandler(e -> {
      logger.error(e.getMessage(), e);
      handleException(oaiPmhResponsePromise, e);
    });

    databaseWriteStream.exceptionHandler(e -> {
      if (e != null) {
        handleException(oaiPmhResponsePromise, e);
      }
    });
    return databaseWriteStream;
  }

  private Promise<Void> saveInstancesIds(List<JsonEvent> instances, String requestId, Context vertxContext) {
    Promise<Void> promise = Promise.promise();
    PostgresClient instance = PostgresClient.getInstance(vertxContext.owner());
    instance.getConnection(e -> {
      List<Tuple> batch = new ArrayList<>();
      List<JsonObject> entities = instances.stream().map(JsonEvent::objectValue).collect(toList());

      for (JsonObject jsonObject : entities) {
        String id = jsonObject.getString("instanceId");
        batch.add(Tuple.of(id, requestId, jsonObject));
      }

      String sql = "INSERT INTO " + "" /*//TODO: this.schemaName*/ + "." + INSTANCES_TABLE_NAME + " (instace_id, request_id, jsonb) VALUES ($1, $2, $3) RETURNING id ";

      PgConnection connection = e.result(); //TODO: ((SQLConnection) e.result()).conn;
      connection.preparedQuery(sql).executeBatch(batch, (queryRes) -> {
        if (queryRes.failed()) {
          promise.fail(queryRes.cause());
        } else {
          promise.complete();
        }
      });
    });
    return promise;
  }

  private HttpClientRequest createInventoryClientRequest(HttpClient httpClient, String inventoryQuery, Request request) {
    logger.info("Sending request to {0}", inventoryQuery);
    final HttpClientRequest httpClientRequest = httpClient
      .getAbs(inventoryQuery);

    httpClientRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpClientRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpClientRequest.putHeader(ACCEPT, APPLICATION_JSON);

    return httpClientRequest;
  }

  private Future<Map<String, JsonObject>> requestSRSByIdentifiers(SourceStorageSourceRecordsClient srsClient,
                                                                  List<JsonObject> batch, boolean deletedRecordSupport) {
    final List<String> listOfIds = extractListOfIdsForSRSRequest(batch);
    logger.info("Request to SRS: {0}", listOfIds);
    Promise<Map<String, JsonObject>> promise = Promise.promise();
    try {
      final Map<String, JsonObject> result = Maps.newHashMap();
      srsClient.postSourceStorageSourceRecords("INSTANCE", deletedRecordSupport, listOfIds, rh -> rh.bodyHandler(bh -> {
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

  private List<String> extractListOfIdsForSRSRequest(List<JsonObject> batch) {

    return batch.stream().
      filter(aggregatedInstanceObject -> aggregatedInstanceObject != null)
      .map(instance -> instance.getString("instanceid"))
      .collect(toList());
  }


  private void handleException(Promise<?> promise, Throwable e) {
    logger.error(e.getMessage(), e);
    promise.fail(e);
  }
}
