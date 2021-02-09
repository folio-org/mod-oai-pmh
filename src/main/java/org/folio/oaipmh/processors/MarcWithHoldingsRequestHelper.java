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
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.client.SourceStorageSourceRecordsClient;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.helpers.AbstractHelper;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.helpers.streaming.BatchStreamWrapper;
import org.folio.oaipmh.service.InstancesService;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.spring.SpringContextUtil;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.StatusType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.ReflectionUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.parsetools.impl.JsonParserImpl;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.impl.Connection;


public class MarcWithHoldingsRequestHelper extends AbstractHelper {

  private static final int DATABASE_FETCHING_CHUNK_SIZE = 50;

  private static final String INSTANCE_ID_FIELD_NAME = "instanceId";

  private static final String ENRICHED_INSTANCE_ID = "instanceid";

  private static final String SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS = "skipSuppressedFromDiscoveryRecords";

  private static final String INSTANCE_IDS_ENRICH_PARAM_NAME = "instanceIds";

  private static final String DELETED_RECORD_SUPPORT_PARAM_NAME = "deletedRecordSupport";

  private static final String START_DATE_PARAM_NAME = "startDate";

  private static final String END_DATE_PARAM_NAME = "endDate";

  private static final String INVENTORY_INSTANCES_ENDPOINT = "/oai-pmh-view/enrichedInstances";

  private static final String INVENTORY_UPDATED_INSTANCES_ENDPOINT = "/inventory-hierarchy/updated-instance-ids";

  private static final int REQUEST_TIMEOUT = 604800000;
  private static final String ERROR_FROM_STORAGE = "Got error response from %s, uri: '%s' message: %s";

  private static final Logger logger = LogManager.getLogger(MarcWithHoldingsRequestHelper.class);

  public static final MarcWithHoldingsRequestHelper INSTANCE = new MarcWithHoldingsRequestHelper();
  private final Vertx vertx;

  public static final int POLLING_TIME_INTERVAL = 500;

  public static final int MAX_WAIT_UNTIL_TIMEOUT = 20000;

  public static final int MAX_POLLING_ATTEMPTS = MAX_WAIT_UNTIL_TIMEOUT / POLLING_TIME_INTERVAL;

  private InstancesService instancesService;
  private final WorkerExecutor saveInstancesExecutor;
  private final Context downloadContext;

  public static MarcWithHoldingsRequestHelper getInstance() {
    return INSTANCE;
  }

  private MarcWithHoldingsRequestHelper() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    vertx = Vertx.vertx();
    downloadContext = vertx.getOrCreateContext();
    saveInstancesExecutor = vertx.createSharedWorkerExecutor("saving-executor", 5);
  }

  /**
   * Handle MarcWithHoldings request
   */
  @Override
  public Future<Response> handle(Request request, Context vertxContext) {
    Promise<Response> promise = Promise.promise();
    try {
      String resumptionToken = request.getResumptionToken();
      List<OAIPMHerrorType> errors = validateListRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, promise, errors);
      }

      String requestId;
      OffsetDateTime lastUpdateDate = OffsetDateTime.now(ZoneId.systemDefault());
      RequestMetadataLb requestMetadata = new RequestMetadataLb()
        .setLastUpdatedDate(lastUpdateDate);

      Future<RequestMetadataLb> updateRequestMetadataFuture;
      if (resumptionToken == null || request.getRequestId() == null) {
        requestId = UUID.randomUUID().toString();
        requestMetadata.setRequestId(UUID.fromString(requestId));
        updateRequestMetadataFuture = instancesService.saveRequestMetadata(requestMetadata, request.getTenant())
        .onFailure(th -> handleException(promise, th));
      } else {
        requestId = request.getRequestId();
        updateRequestMetadataFuture = instancesService.updateRequestUpdatedDate(requestId, lastUpdateDate, request.getTenant())
          .onFailure(th -> handleException(promise, th));
      }

      updateRequestMetadataFuture.onSuccess(res -> {
        boolean isFirstBatch = resumptionToken == null || request.getRequestId() == null;
        processBatch(request, vertxContext, promise, requestId, isFirstBatch);
        if (isFirstBatch) {
          saveInstancesExecutor.executeBlocking(
            blockingFeature -> downloadInstances(request, promise, blockingFeature, downloadContext, requestId),
            asyncResult -> {
              instancesService.updateRequestStreamEnded(requestId, true, request.getTenant());
              logger.info("Downloading instances complete");
            });
        }
      }).onFailure(th -> handleException(promise, th));
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  private void processBatch(Request request, Context context, Promise<Response> oaiPmhResponsePromise, String requestId, boolean firstBatch) {
    try {
      boolean deletedRecordSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request);
      int batchSize = Integer.parseInt(
        RepositoryConfigurationUtil.getProperty(request.getTenant(),
          REPOSITORY_MAX_RECORDS_PER_RESPONSE));

      getNextInstances(request, batchSize, context, requestId).future().onComplete(fut -> {
        if (fut.failed()) {
          logger.error("Get instances failed:", fut.cause());
          oaiPmhResponsePromise.fail(fut.cause());
          return;
        }
        List<JsonObject> instances = fut.result();
        logger.info("Processing instances: {}", instances.size());
        if (CollectionUtils.isEmpty(instances) && !firstBatch) {
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
            .onSuccess(oaiPmhResponsePromise::complete)
            .onFailure(e -> handleException(oaiPmhResponsePromise, e));
          return;
        }

        String nextInstanceId = instances.size() < batchSize ? null : instances.get(batchSize).getString(INSTANCE_ID_FIELD_NAME);
        List<JsonObject> instancesWithoutLast = nextInstanceId != null ? instances.subList(0, batchSize) : instances;
        final SourceStorageSourceRecordsClient srsClient = new SourceStorageSourceRecordsClient(request.getOkapiUrl(),
          request.getTenant(), request.getOkapiToken());


        requestSRSByIdentifiers(srsClient, instancesWithoutLast, deletedRecordSupport).compose(res ->
          buildRecordsResponse(request, requestId, instancesWithoutLast, res,
          firstBatch, nextInstanceId, deletedRecordSupport)
            .onSuccess(result -> {
          List<String> instanceIds = instancesWithoutLast.stream()
            .map(e -> e.getString(INSTANCE_ID_FIELD_NAME))
            .collect(toList());
          instancesService.deleteInstancesById(instanceIds, requestId, request.getTenant())
            .onComplete(r -> oaiPmhResponsePromise.complete(result));
        }).onFailure(e -> handleException(oaiPmhResponsePromise, e)))
        .onFailure(e -> handleException(oaiPmhResponsePromise, e));
      });
    } catch (Exception e) {
      handleException(oaiPmhResponsePromise, e);
    }
  }

  private void downloadInstances(Request request, Promise<Response> oaiPmhResponsePromise, Promise<Object> downloadInstancesPromise,
      Context vertxContext, String requestId) {
    final HttpClientOptions options = new HttpClientOptions();
    options.setKeepAliveTimeout(REQUEST_TIMEOUT);
    options.setConnectTimeout(REQUEST_TIMEOUT);
    HttpClient httpClient = vertxContext.owner()
      .createHttpClient(options);

    buildInventoryQuery(httpClient, request).onComplete(res -> {
      if (res.failed()) {
        logger.error(res.cause().getMessage(), res.cause());
        oaiPmhResponsePromise.fail(res.cause());
        return;
      }
      HttpClientRequest httpClientRequest = res.result();
      httpClientRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
      httpClientRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
      httpClientRequest.putHeader(ACCEPT, APPLICATION_JSON);

      httpClientRequest.setTimeout(REQUEST_TIMEOUT);
      PostgresClient postgresClient = PostgresClient.getInstance(vertxContext.owner(), request.getTenant());
      BatchStreamWrapper databaseWriteStream = new BatchStreamWrapper(vertxContext.owner(), DATABASE_FETCHING_CHUNK_SIZE);

      databaseWriteStream.handleBatch(batch -> {
        saveInstancesIds(batch, request, requestId, databaseWriteStream, postgresClient);
        final Long returnedCount = databaseWriteStream.getReturnedCount();

        if (returnedCount % 1000 == 0) {
          logger.info("Batch saving progress: {} returned so far, batch size: {}, http ended: {}", returnedCount, batch.size(),
              databaseWriteStream.isStreamEnded());
        }

        if (databaseWriteStream.isTheLastBatch()) {
          downloadInstancesPromise.complete();
        }

        databaseWriteStream.invokeDrainHandler();
      });
      setupBatchHttpStream(databaseWriteStream, oaiPmhResponsePromise, httpClientRequest, (PgPool) getValueFrom(postgresClient, "client"), httpClient);
    });
  }

  private Future<HttpClientRequest> buildInventoryQuery(HttpClient httpClient, Request request) {
    Map<String, String> paramMap = new HashMap<>();
    Date date = convertStringToDate(request.getFrom(), false, false);
    if (date != null) {
      paramMap.put(START_DATE_PARAM_NAME, dateFormat.format(date));
    }
    date = convertStringToDate(request.getUntil(), true, false);
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

    String inventoryQuery = format("%s?%s",  INVENTORY_UPDATED_INSTANCES_ENDPOINT, params);

    logger.info("Sending request to : {}", inventoryQuery);


    List<String> okapiUrlParts = Splitter.on(":").splitToList(request.getOkapiUrl());
    String okapiHost = okapiUrlParts.get(1).replace("//","");
    Integer okapiPort = Integer.valueOf(okapiUrlParts.get(2));

    return httpClient.request(HttpMethod.GET, okapiPort, okapiHost, inventoryQuery);
  }

  private void setupBatchHttpStream(BatchStreamWrapper databaseWriteStream, Promise<?> promise, HttpClientRequest inventoryQuery, PgPool pool, HttpClient inventoryHttpClient) {

    AtomicReference<ArrayDeque<Promise<Connection>>> queue = new AtomicReference<>();
    try {
      queue.set(getWaitersQueue(pool));
    } catch (IllegalStateException ex) {
      logger.error(ex.getMessage());
      promise.fail(ex);
    }

    databaseWriteStream.setCapacityChecker(() -> queue.get()
      .size() > 20);

    inventoryQuery.send(resp -> {
      if (resp.failed()) {
        logger.error(resp.cause().getMessage(), resp.cause());
        handleException(promise, resp.cause());
        inventoryHttpClient.close();
        return;
      }
      final HttpClientResponse result = resp.result();
      if (result.statusCode() != 200) {
        String errorMsg = getErrorFromStorageMessage("inventory-storage", inventoryQuery.absoluteURI(), result.statusMessage());
        promise.fail(new IllegalStateException(errorMsg));
        inventoryHttpClient.close();
      } else {
        JsonParser jp = new JsonParserImpl(resp.result());
        jp.objectValueMode();
        jp.pipeTo(databaseWriteStream);
        jp.endHandler(e -> {
          databaseWriteStream.end();
          inventoryHttpClient.close();
        })
          .exceptionHandler(throwable -> {
            logger.error("Error has been occurred at JsonParser while reading data from response. Message: {}", throwable.getMessage(), throwable);
            databaseWriteStream.end();
            inventoryHttpClient.close();
            promise.fail(throwable);
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
  }

  /**
   * If some error occures while parsing an invalid json response from inv.-storage the getNextBatch
   * will not be informed about that and it will be continued in an infinite loop.
   *
   * NEED to establish the error feedback between downloading instances and requesting batches.
   */
  private Promise<List<JsonObject>> getNextInstances(Request request, int batchSize, Context context, String requestId) {
    Promise<List<JsonObject>> promise = Promise.promise();


    final Promise<List<Instances>> listPromise = Promise.promise();

    AtomicInteger retryCount = new AtomicInteger();

    context.owner().setPeriodic(POLLING_TIME_INTERVAL, timer -> getNextBatch(requestId, request, batchSize, listPromise, context, timer, retryCount));

    listPromise.future()
      .compose(instances -> {
        List<JsonObject> jsonInstances = instances.stream()
          .map(Instances::getJson)
          .map(JsonObject::new)
          .collect(Collectors.toList());
        return enrichInstances(jsonInstances, request, context);
      }).onComplete(asyncResult -> {
        if (asyncResult.succeeded()) {
          promise.complete(asyncResult.result());
        } else {
          logger.error("Cannot save ids: {}", asyncResult.cause().getMessage(), asyncResult.cause());
        promise.fail(asyncResult.cause());
        }
      });

    return promise;
  }

  private Future<List<Instances>> getNextBatch(String requestId, Request request, int batchSize, Promise<List<Instances>> listPromise, Context context, Long timerId, AtomicInteger retryCount) {
    if (retryCount.incrementAndGet() > MAX_POLLING_ATTEMPTS) {
      return Future.failedFuture("The instance list is empty after "+retryCount.get()+" attempts. Stop polling and return fail response");
    }
    return instancesService.getRequestMetadataByRequestId(requestId, request.getTenant())
      .compose(requestMetadata -> Future.succeededFuture(requestMetadata.getStreamEnded()))
      .compose(streamEnded -> instancesService.getInstancesList(batchSize + 1, requestId, request.getTenant())
        .onComplete(f -> {
          if (f.succeeded()) {
            if (!listPromise.future().isComplete() && (f.result().size() == batchSize + 1 || streamEnded)) {
              context.owner().cancelTimer(timerId);
              listPromise.complete(f.result());
            }
          } else {
            logger.error(f.cause());
          }
        }));
  }

  private Future<List<JsonObject>> enrichInstances(List<JsonObject> result, Request request, Context context) {
    Map<String, JsonObject> instances = result.stream().collect(toMap(e -> e.getString(INSTANCE_ID_FIELD_NAME), Function.identity()));
    Promise<List<JsonObject>> completePromise = Promise.promise();
    HttpClient httpClient = context.owner().createHttpClient();

    createInventoryPostRequest(httpClient, request).onComplete(httpClientRequest -> {
      if(httpClientRequest.failed()) {
        logger.error(httpClientRequest.cause().getMessage(),httpClientRequest.cause());
        completePromise.fail(httpClientRequest.cause());
      }
      HttpClientRequest enrichInventoryClientRequest = httpClientRequest.result();
      enrichInventoryClientRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
      enrichInventoryClientRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
      enrichInventoryClientRequest.putHeader(ACCEPT, APPLICATION_JSON);
      enrichInventoryClientRequest.putHeader(CONTENT_TYPE, APPLICATION_JSON);

      BatchStreamWrapper enrichedInstancesStream = new BatchStreamWrapper(context.owner(), DATABASE_FETCHING_CHUNK_SIZE);

      JsonObject entries = new JsonObject();
      entries.put(INSTANCE_IDS_ENRICH_PARAM_NAME, new JsonArray(new ArrayList<>(instances.keySet())));
      entries.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS, isSkipSuppressed(request));
      enrichInventoryClientRequest.send(entries.encode())
        .onSuccess(httpClientResponse -> writeResponseToStream(httpClient, completePromise, enrichInventoryClientRequest,
          enrichedInstancesStream, httpClientResponse))
        .onFailure(e -> {
          logger.error(e.getMessage());
          completePromise.fail(e);
        });


      enrichedInstancesStream.handleBatch(batch -> {
        try {
          for (JsonEvent jsonEvent : batch) {
            JsonObject value = jsonEvent.objectValue();
            String instanceId = value.getString(ENRICHED_INSTANCE_ID);
            Object itemsandholdingsfields = value.getValue(RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS);
            if (itemsandholdingsfields instanceof JsonObject) {
            JsonObject instance = instances.get(instanceId);
            if (instance != null) {
              enrichDiscoverySuppressed((JsonObject) itemsandholdingsfields, instance);
              instance.put(RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS,
                itemsandholdingsfields);
            } else {
              logger.info(format("Instance with instanceId %s wasn't in the request", instanceId));
            }
          }
        }

        if (enrichedInstancesStream.isTheLastBatch() && !completePromise.future().isComplete()) {
          completePromise.complete(new ArrayList<>(instances.values()));
        }
      } catch (Exception e) {
        completePromise.fail(e);
      }
    });
      setupBatchHttpStream(enrichedInstancesStream, completePromise, enrichInventoryClientRequest, PostgresClientFactory.getPool(context.owner(), request.getTenant()), httpClient);
      });
    return completePromise.future();
  }

  private void writeResponseToStream(HttpClient inventoryHttpClient, Promise<?> promise, HttpClientRequest inventoryQuery, BatchStreamWrapper databaseWriteStream, HttpClientResponse resp) {
    if (resp.statusCode() != 200) {
      String errorFromStorageMessage = getErrorFromStorageMessage("inventory-storage", inventoryQuery.absoluteURI(), resp.statusMessage());
      String errorMessage = errorFromStorageMessage + resp.statusCode() + "body: {}";
      resp.bodyHandler(buffer -> logger.error(errorMessage, buffer.toString()));
      promise.fail(new IllegalStateException(errorFromStorageMessage));
    } else {
      JsonParser jp = new JsonParserImpl(resp);
      jp.objectValueMode();
      jp.pipeTo(databaseWriteStream);
      jp.endHandler(e -> closeStreamRelatedObjects(databaseWriteStream, inventoryHttpClient))
        .exceptionHandler(throwable -> {
          logger.error("Error has been occurred at JsonParser while reading data from response. Message:{}", throwable.getMessage(), throwable);
          closeStreamRelatedObjects(databaseWriteStream, inventoryHttpClient);
          promise.fail(throwable);
        });
    }
  }

  private void closeStreamRelatedObjects(BatchStreamWrapper databaseWriteStream, HttpClient inventoryHttpClient) {
    databaseWriteStream.end();
    inventoryHttpClient.close();
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

  private Future<Response> buildRecordsResponse(
    Request request, String requestId, List<JsonObject> batch,
    Map<String, JsonObject> srsResponse, boolean firstBatch,
    String nextInstanceId, boolean deletedRecordSupport) {

    Promise<Response> promise = Promise.promise();
    try {
      List<RecordType> records = buildRecordsList(request, batch, srsResponse, deletedRecordSupport);

      logger.info("Build records response, instances = {}, instances with srs records = {}", batch.size(), records.size());
      ResponseHelper responseHelper = getResponseHelper();
      OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request);
      if (records.isEmpty() && nextInstanceId == null && firstBatch) {
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
    batch.stream()
      .filter(instance -> {
          final String instanceId = instance.getString(INSTANCE_ID_FIELD_NAME);
          final JsonObject srsInstance = srsResponse.get(instanceId);
          return Objects.nonNull(srsInstance);
        }
      ).forEach(instance -> {
      final String instanceId = instance.getString(INSTANCE_ID_FIELD_NAME);
      final JsonObject srsInstance = srsResponse.get(instanceId);
      RecordType record = createRecord(request, instance, instanceId);

      JsonObject updatedSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, instance, suppressedRecordsProcessing);
      if (deletedRecordSupport && storageHelper.isRecordMarkAsDeleted(updatedSrsInstance)) {
        record.getHeader().setStatus(StatusType.DELETED);
      }
      String source = storageHelper.getInstanceRecordSource(updatedSrsInstance);
      if (source != null && record.getHeader().getStatus() == null) {
        if (suppressedRecordsProcessing) {
          source = metadataManager.updateMetadataSourceWithDiscoverySuppressedData(source, updatedSrsInstance);
          source = metadataManager.updateElectronicAccessFieldWithDiscoverySuppressedData(source, updatedSrsInstance);
        }
        try {
          record.withMetadata(buildOaiMetadata(request, source));
        } catch (Exception e) {
          logger.error("Error occurred while converting record to xml representation. {}", e.getMessage(), e);
          logger.debug("Skipping problematic record due the conversion error. Source record id - {}", storageHelper.getRecordId(srsInstance));
          return;
        }
      }
      if (filterInstance(request, srsInstance)) {
        records.add(record);
      }
    });
    return records;
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

  private RecordType createRecord(Request request, JsonObject instance, String instanceId) {
    String identifierPrefix = request.getIdentifierPrefix();
    return new RecordType()
      .withHeader(createHeader(instance, request)
        .withIdentifier(getIdentifier(identifierPrefix, instanceId)));
  }

  private boolean isSkipSuppressed(Request request) {
    return !getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
  }

  /**
   * Here the reflection is used by the reason that we need to have an access to vert.x "waiters" objects
   * which are accumulated when batches are saved to database, the handling batches from inventory view
   * is performed match faster versus saving to database. By this reason in some time we got a lot of
   * waiters objects which holds many of others as well and this leads to OutOfMemory.
   * In solution we just don't allow to request new batches while we have 20 waiters objects
   * which perform saving instances to DB.
   * In future we can consider using static AtomicInteger to count the number of current db requests.
   * It will be more readable in code, but less reliable because wouldn't take into account other requests.
   */
  private Object getValueFrom(Object obj, String fieldName) {
    Field field = requireNonNull(ReflectionUtils.findField(requireNonNull(obj.getClass()), fieldName));
    ReflectionUtils.makeAccessible(field);
    return ReflectionUtils.getField(field, obj);
  }

  private ArrayDeque<Promise<Connection>> getWaitersQueue(PgPool pgPool) {
    if (Objects.nonNull(pgPool)) {
      try {
        return (ArrayDeque<Promise<Connection>>) getValueFrom(getValueFrom(pgPool, "pool"), "waiters");
      } catch (NullPointerException ex) {
        throw new IllegalStateException("Cannot get the pool size. Object for retrieving field is null.");
      }
    } else {
      throw new IllegalStateException("Cannot obtain the pool. Pool is null.");
    }
  }

  private Promise<Void> saveInstancesIds(List<JsonEvent> instances, Request request, String requestId, BatchStreamWrapper databaseWriteStream, PostgresClient postgresClient) {
    Promise<Void> promise = Promise.promise();
    List<Instances> instancesList = toInstancesList(instances, UUID.fromString(requestId));
    saveInstances(instancesList, request.getTenant(), requestId, postgresClient).onComplete(res -> {
      if (res.failed()) {
        logger.error("Cannot saving ids, error from database: {}", res.cause().getMessage(), res.cause());
        promise.fail(res.cause());
      } else {
        promise.complete();
        databaseWriteStream.invokeDrainHandler();
      }
    });
    return promise;
  }

  private Future<Void> saveInstances(List<Instances> instances, String tenantId, String requestId, PostgresClient postgresClient) {
    if (instances.isEmpty()) {
      logger.debug("Skip saving instances. Instances list is empty.");
      return Future.succeededFuture();
    }

    Promise<Void> promise = Promise.promise();
    postgresClient.getConnection(e -> {
      List<Tuple> batch = new ArrayList<>();
      instances.forEach(inst -> batch.add(Tuple.of(inst.getInstanceId(), UUID.fromString(requestId), inst.getJson())));

      String sql = "INSERT INTO " + PostgresClient.convertToPsqlStandard(tenantId) + ".instances (instance_id, request_id, json) VALUES ($1, $2, $3) RETURNING instance_id";

      if (e.failed()) {
        logger.error("Save instance Ids failed: {}", e.cause().getMessage());
        promise.fail(e.cause());
      } else {
        PgConnection connection = e.result();
        connection.preparedQuery(sql).executeBatch(batch, queryRes -> {
          connection.close();
          if (queryRes.failed()) {
            promise.fail(queryRes.cause());
          } else {
            promise.complete();
          }
        });
      }
    });
    return promise.future();
  }

  private List<Instances> toInstancesList(List<JsonEvent> jsonEventInstances, UUID requestId) {
    return jsonEventInstances.stream().map(JsonEvent::objectValue).map(inst ->
      new Instances().setInstanceId(UUID.fromString(inst.getString(INSTANCE_ID_FIELD_NAME)))
        .setJson(inst.toString())
        .setRequestId(requestId)
    ).collect(Collectors.toList());
  }

  private Future<HttpClientRequest> createInventoryPostRequest(HttpClient httpClient, Request request) {
    List<String> okapiUrlParts = Splitter.on(":").splitToList(request.getOkapiUrl());
    String okapiHost = okapiUrlParts.get(1).replace("//","");
    Integer okapiPort = Integer.valueOf(okapiUrlParts.get(2));
    return httpClient.request(HttpMethod.POST, okapiPort, okapiHost, INVENTORY_INSTANCES_ENDPOINT);
  }

  private Future<Map<String, JsonObject>> requestSRSByIdentifiers(SourceStorageSourceRecordsClient srsClient,
      List<JsonObject> batch, boolean deletedRecordSupport) {
    final List<String> listOfIds = extractListOfIdsForSRSRequest(batch);
    logger.info("Request to SRS: {}", listOfIds);
    Promise<Map<String, JsonObject>> promise = Promise.promise();
    try {
      final Map<String, JsonObject> result = Maps.newHashMap();
      srsClient.postSourceStorageSourceRecords("INSTANCE", deletedRecordSupport, listOfIds)
        .onSuccess(srsResp -> {
          if (srsResp.statusCode() != 200) {
            String errorMsg = getErrorFromStorageMessage("source-record-storage", "/source-storage/source-records",
                srsResp.statusMessage());
            srsClient.close();
            handleException(promise, new IllegalStateException(errorMsg));
            return;
          }
          try {
            final Object o = srsResp.bodyAsJsonObject();
            if (o instanceof JsonObject) {
              JsonObject entries = (JsonObject) o;
              final JsonArray records = entries.getJsonArray("sourceRecords");
              records.stream()
                .filter(Objects::nonNull)
                .map(JsonObject.class::cast)
                .forEach(jo -> result.put(jo.getJsonObject("externalIdsHolder")
                  .getString(INSTANCE_ID_FIELD_NAME), jo));
            } else {
              logger.debug("Can't process response from SRS: {}", srsResp);
            }
            promise.complete(result);
          } catch (DecodeException ex) {
            String msg = "Invalid json has been returned from SRS, cannot parse response to json.";
            handleException(promise, new IllegalStateException(msg, ex));
          } catch (Exception e) {
            handleException(promise, e);
          } finally {
            srsClient.close();
          }
        })
        .onFailure(e -> {
          srsClient.close();
          handleException(promise, e);
        });
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  private String getErrorFromStorageMessage(String errorSource, String uri, String responseMessage) {
    return format(ERROR_FROM_STORAGE, errorSource, uri, responseMessage);
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

  @Autowired
  public void setInstancesService(InstancesService instancesService) {
    this.instancesService = instancesService;
  }

}
