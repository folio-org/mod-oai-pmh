package org.folio.oaipmh.processors;

import com.google.common.collect.Maps;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.impl.pool.SimpleConnectionPool;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.client.impl.HttpRequestImpl;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.helpers.AbstractHelper;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.helpers.streaming.BatchStreamWrapper;
import org.folio.oaipmh.service.InstancesService;
import org.folio.rest.client.SourceStorageSourceRecordsClient;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.tools.utils.VertxUtils;
import org.folio.spring.SpringContextUtil;
import org.openarchives.oai._2.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.ReflectionUtils;

import javax.sql.PooledConnection;
import javax.ws.rs.core.Response;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.*;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;


public class MarcWithHoldingsRequestHelper extends AbstractHelper {

  private static final int DATABASE_FETCHING_CHUNK_SIZE = 50;

  private static final String INSTANCE_ID_FIELD_NAME = "instanceId";

  private static final String ENRICHED_INSTANCE_ID = "instanceid";

  private static final String SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS = "skipSuppressedFromDiscoveryRecords";

  private static final String INSTANCE_IDS_ENRICH_PARAM_NAME = "instanceIds";

  private static final String DELETED_RECORD_SUPPORT_PARAM_NAME = "deletedRecordSupport";

  private static final String START_DATE_PARAM_NAME = "startDate";

  private static final String END_DATE_PARAM_NAME = "endDate";

  private static final String INVENTORY_ENRICHED_INSTANCES_ENDPOINT = "/oai-pmh-view/enrichedInstances";

  private static final String INVENTORY_UPDATED_INSTANCES_ENDPOINT = "/inventory-hierarchy/updated-instance-ids";

  private static final int REQUEST_TIMEOUT = 604800000;
  private static final String ERROR_FROM_STORAGE = "Got error response from %s, uri: '%s' message: %s";
  public static final int RESPONSE_CHECK_ATTEMPTS = 30;

  protected final Logger logger = LogManager.getLogger(getClass());

  public static final MarcWithHoldingsRequestHelper INSTANCE = new MarcWithHoldingsRequestHelper();
  private final Vertx vertx;

  private static final int REREQUEST_SRS_DELAY = 2000;
  private static final int DEFAULT_IDLE_TIMEOUT_SEC = 20;
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 2000;
  private static final String GET_IDLE_TIMEOUT_ERROR_MESSAGE = "Error occurred during resolving the idle timeout setting value. Setup client with default idle timeout " + DEFAULT_IDLE_TIMEOUT_SEC + " seconds.";

  private static final int POLLING_TIME_INTERVAL = 500;
  private static final int MAX_WAIT_UNTIL_TIMEOUT = 1000*60*20;
  private static final int MAX_POLLING_ATTEMPTS = MAX_WAIT_UNTIL_TIMEOUT / POLLING_TIME_INTERVAL;

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
    Promise<Response> oaipmhResponsePromise = Promise.promise();
    try {
      String resumptionToken = request.getResumptionToken();
      List<OAIPMHerrorType> errors = validateListRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, oaipmhResponsePromise, errors);
      }

      String requestId;
      OffsetDateTime lastUpdateDate = OffsetDateTime.now(ZoneId.systemDefault());
      RequestMetadataLb requestMetadata = new RequestMetadataLb()
        .setLastUpdatedDate(lastUpdateDate);

      Future<RequestMetadataLb> updateRequestMetadataFuture;
      if (resumptionToken == null) {
        requestId = request.getRequestId();
        requestMetadata.setRequestId(UUID.fromString(requestId));
        updateRequestMetadataFuture = instancesService.saveRequestMetadata(requestMetadata, request.getTenant());
      } else {
        requestId = request.getRequestId();
        updateRequestMetadataFuture = instancesService.updateRequestUpdatedDate(requestId, lastUpdateDate, request.getTenant());
      }
      updateRequestMetadataFuture.onSuccess(res -> {
        boolean isFirstBatch = resumptionToken == null;
        processBatch(request, vertxContext, oaipmhResponsePromise, requestId, isFirstBatch);
        if (isFirstBatch) {
          saveInstancesExecutor.executeBlocking(
            downloadInstancesPromise -> downloadInstances(request, oaipmhResponsePromise, downloadInstancesPromise, downloadContext, requestId),
            downloadInstancesResult -> {
              instancesService.updateRequestStreamEnded(requestId, true, request.getTenant());
              if (downloadInstancesResult.succeeded()) {
                logger.info("Downloading instances complete.");
              } else {
                logger.error("Downloading instances was canceled due to the error. ", downloadInstancesResult.cause());
                if (!oaipmhResponsePromise.future().isComplete()) {
                  oaipmhResponsePromise.fail(new IllegalStateException(downloadInstancesResult.cause()));
                }
              }
            });
        }
      }).onFailure(th -> handleException(oaipmhResponsePromise, th));
    } catch (Exception e) {
      handleException(oaipmhResponsePromise, e);
    }
    return oaipmhResponsePromise.future();
  }

  private void processBatch(Request request, Context context, Promise<Response> oaiPmhResponsePromise, String requestId, boolean firstBatch) {
    try {
      boolean deletedRecordSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request.getRequestId());
      int batchSize = Integer.parseInt(
        RepositoryConfigurationUtil.getProperty(request.getRequestId(),
          REPOSITORY_MAX_RECORDS_PER_RESPONSE));

      getNextInstances(request, batchSize, context, requestId, firstBatch).future().onComplete(fut -> {
        if (fut.failed()) {
          logger.error("Get instances failed: {}.", fut.cause().getMessage(), fut.cause());
          oaiPmhResponsePromise.fail(fut.cause());
          return;
        }

        List<JsonObject> instances = fut.result();
        logger.info("Processing instances: {}.", instances.size());
        if (CollectionUtils.isEmpty(instances) && !firstBatch) {
          handleException(oaiPmhResponsePromise, new IllegalArgumentException(
            "Specified resumption token doesn't exists."));
          return;
        }

        if (!firstBatch && (CollectionUtils.isNotEmpty(instances) && !instances.get(0).getString(INSTANCE_ID_FIELD_NAME).equals(request.getNextRecordId()))) {
          handleException(oaiPmhResponsePromise, new IllegalArgumentException("Stale resumption token."));
          return;
        }

        if (CollectionUtils.isEmpty(instances)) {
          logger.debug("Got empty instances.");
          buildRecordsResponse(request, requestId, instances, new HashMap<>(),
            firstBatch, null, deletedRecordSupport)
            .onSuccess(oaiPmhResponsePromise::complete)
            .onFailure(e -> handleException(oaiPmhResponsePromise, e));
          return;
        }

        String nextInstanceId = instances.size() <= batchSize ? null : instances.get(batchSize).getString(INSTANCE_ID_FIELD_NAME);
        List<JsonObject> instancesWithoutLast = nextInstanceId != null ? instances.subList(0, batchSize) : instances;
        final SourceStorageSourceRecordsClient srsClient = createAndSetupSrsClient(request);

        int retryAttempts = Integer.parseInt(RepositoryConfigurationUtil.getProperty(request.getRequestId(), REPOSITORY_SRS_HTTP_REQUEST_RETRY_ATTEMPTS));
        requestSRSByIdentifiers(srsClient, context.owner(), instancesWithoutLast, deletedRecordSupport, retryAttempts)
          .onSuccess(res -> buildRecordsResponse(request, requestId, instancesWithoutLast, res,
          firstBatch, nextInstanceId, deletedRecordSupport).onSuccess(oaiPmhResponsePromise::complete)
            .onFailure(e -> handleException(oaiPmhResponsePromise, e)))
        .onFailure(e -> handleException(oaiPmhResponsePromise, e));
      });
    } catch (Exception e) {
      handleException(oaiPmhResponsePromise, e);
    }
  }

  private SourceStorageSourceRecordsClient createAndSetupSrsClient(Request request) {
    String property = System.getProperty(REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC);
    final String defaultValue = Objects.nonNull(property) ? property : String.valueOf(DEFAULT_IDLE_TIMEOUT_SEC);
    String val = Optional.ofNullable(Vertx.currentContext()
      .config()
      .getJsonObject(request.getTenant()))
      .map(config -> config.getString(REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC, defaultValue))
      .orElse(defaultValue);
    int idleTimeout = DEFAULT_IDLE_TIMEOUT_SEC;
    try {
      idleTimeout = Integer.parseInt(val);
      logger.debug("Setup client with idle timeout '{}' seconds", idleTimeout);
    } catch (Exception e) {
      logger.error(GET_IDLE_TIMEOUT_ERROR_MESSAGE, e);
    }
    return new SourceStorageSourceRecordsClient(request.getOkapiUrl(), request.getTenant(), request.getOkapiToken(), true,
      DEFAULT_CONNECTION_TIMEOUT_MS, idleTimeout);
  }

  private void downloadInstances(Request request,
                                 Promise<Response> oaiPmhResponsePromise, Promise<Object> downloadInstancesPromise,
                                 Context vertxContext, String requestId) {
    final var options = new WebClientOptions();
    options.setKeepAliveTimeout(REQUEST_TIMEOUT);
    options.setConnectTimeout(REQUEST_TIMEOUT);
    var webClient = WebClient.create(vertxContext.owner(), options);
    HttpRequestImpl<Buffer> httpRequest = (HttpRequestImpl<Buffer>) buildInventoryQuery(webClient, request);
    var databaseWriteStream = new BatchStreamWrapper(DATABASE_FETCHING_CHUNK_SIZE);
    PostgresClient postgresClient = PostgresClient.getInstance(vertxContext.owner(), request.getTenant());

    databaseWriteStream.handleBatch(batch -> {
      saveInstancesIds(batch, request, requestId, databaseWriteStream, postgresClient);
      final Long returnedCount = databaseWriteStream.getReturnedCount();

      if (returnedCount % 1000 == 0) {
        logger.info("Batch saving progress: {} returned so far, batch size: {}, http ended: {}.", returnedCount, batch.size(), databaseWriteStream.isStreamEnded());
      }

      if (databaseWriteStream.isTheLastBatch()) {
        if (databaseWriteStream.isEndedWithError()) {
          downloadInstancesPromise.fail(databaseWriteStream.getCause());
        } else {
          downloadInstancesPromise.complete();
        }
      }

      databaseWriteStream.invokeDrainHandler();
    });
    setupBatchHttpStream(databaseWriteStream, oaiPmhResponsePromise, httpRequest,
      (PgPool) getValueFrom(postgresClient, "client"), webClient);
  }

  private void setupBatchHttpStream(BatchStreamWrapper databaseWriteStream, Promise<?> promise, HttpRequestImpl<Buffer> inventoryHttpRequest, PgPool pool, WebClient webClient) {

    AtomicReference<SimpleConnectionPool<PooledConnection>> connectionPool = new AtomicReference<>();
    try {
      connectionPool.set(getWaitersQueue(pool));
    } catch (IllegalStateException ex) {
      logger.error(ex.getMessage());
      promise.fail(ex);
    }
    databaseWriteStream.setCapacityChecker(() -> connectionPool.get().waiters() > 20);

    var responseChecked = new AtomicBoolean();
    var responseCheckAttempts = new AtomicInteger(30);
    var jsonParser = JsonParser.newParser().objectValueMode();
    jsonParser.pipeTo(databaseWriteStream);
    jsonParser.endHandler(e -> closeStreamRelatedObjects(databaseWriteStream, webClient, responseChecked, responseCheckAttempts));
    jsonParser.exceptionHandler(throwable -> {
      logger.error("Error has been occurred at JsonParser while reading data from response. Message: {}", throwable.getMessage(), throwable);
      closeStreamRelatedObjects(databaseWriteStream, webClient, Optional.of(throwable));
      promise.fail(throwable);
    });

    inventoryHttpRequest.as(BodyCodec.jsonStream(jsonParser))
      .send()
      .onSuccess(resp -> {
        if (resp.statusCode() != 200) {
          String errorMsg = getErrorFromStorageMessage("inventory-storage", inventoryHttpRequest.uri(), resp.statusMessage());
          closeStreamRelatedObjects(databaseWriteStream, webClient, Optional.empty());
          responseChecked.set(true);
          promise.fail(new IllegalStateException(errorMsg));
        }
        responseChecked.set(true);
      })
      .onFailure(throwable -> {
        logger.error("Error has been occurred at JsonParser while reading data from response. Message: {}", throwable.getMessage(), throwable);
        closeStreamRelatedObjects(databaseWriteStream, webClient, Optional.of(throwable));
        responseChecked.set(true);
        promise.fail(throwable);
      });

    databaseWriteStream.exceptionHandler(e -> {
      if (e != null) {
        handleException(promise, e);
      }
    });
  }

  private HttpRequest<Buffer> buildInventoryQuery(WebClient webClient, Request request) {
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
        RepositoryConfigurationUtil.isDeletedRecordsEnabled(request.getRequestId())));
    paramMap.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS,
      String.valueOf(
        isSkipSuppressed(request)));

    final String params = paramMap.entrySet().stream()
      .map(e -> e.getKey() + "=" + e.getValue())
      .collect(Collectors.joining("&"));

    String inventoryQuery = format("%s%s?%s", request.getOkapiUrl(), INVENTORY_UPDATED_INSTANCES_ENDPOINT, params);

    logger.info("Sending request to {}", inventoryQuery);
    final HttpRequest<Buffer> httpRequest = webClient.getAbs(inventoryQuery);

    httpRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpRequest.putHeader(ACCEPT, APPLICATION_JSON);
    httpRequest.timeout(REQUEST_TIMEOUT);
    if (request.getOkapiUrl().contains("https")) {
      httpRequest.ssl(true);
    }
    return httpRequest;
  }

  private Promise<List<JsonObject>> getNextInstances(Request request, int batchSize, Context context, String requestId, boolean firstBatch) {
    Promise<List<JsonObject>> promise = Promise.promise();
    final Promise<List<Instances>> listPromise = Promise.promise();
    AtomicInteger retryCount = new AtomicInteger();
    vertx.setTimer(200, id -> getNextBatch(requestId, request, firstBatch, batchSize, listPromise, context, retryCount));
    listPromise.future()
      .compose(instances -> {
        if (CollectionUtils.isNotEmpty(instances)) {
          List<JsonObject> jsonInstances = instances.stream()
            .map(Instances::getJson)
            .map(JsonObject::new)
            .collect(Collectors.toList());
          if (instances.size() > batchSize) {
            request.setNextInstancePkValue(instances.get(batchSize).getId());
          }
          return enrichInstances(jsonInstances, request, context);
        }
        logger.debug("Skipping enrich instances call, empty instance ids list returned.");
        return Future.succeededFuture(Collections.emptyList());
      }).onSuccess(promise::complete)
      .onFailure(throwable -> {
        logger.error("Cannot get batch of instances ids from database: {}.", throwable.getMessage(), throwable);
        promise.fail(throwable);
      });

    return promise;
  }

  private void getNextBatch(String requestId, Request request, boolean firstBatch, int batchSize, Promise<List<Instances>> listPromise, Context context,
                            AtomicInteger retryCount) {
    if (retryCount.incrementAndGet() > MAX_POLLING_ATTEMPTS) {
      listPromise.fail(new IllegalStateException("The instance list is empty after " + retryCount.get() + " attempts. Stop polling and return fail response."));
      return;
    }
    instancesService.getRequestMetadataByRequestId(requestId, request.getTenant())
      .compose(requestMetadata -> Future.succeededFuture(requestMetadata.getStreamEnded()))
      .compose(streamEnded ->
      {
        if (firstBatch) {
          return instancesService.getInstancesList(batchSize + 1, requestId, request.getTenant())
            .onComplete(handleInstancesDbResponse(listPromise, streamEnded, batchSize, timer -> getNextBatch(requestId, request, firstBatch, batchSize, listPromise, context, retryCount)));
        }
        int autoIncrementedId = request.getNextInstancePkValue();
        return instancesService.getInstancesList(batchSize + 1, requestId, autoIncrementedId, request.getTenant())
          .onComplete(handleInstancesDbResponse(listPromise, streamEnded, batchSize, timer -> getNextBatch(requestId, request, firstBatch, batchSize, listPromise, context, retryCount)));
      });
  }

  private Handler<AsyncResult<List<Instances>>> handleInstancesDbResponse(Promise<List<Instances>> listPromise, boolean streamEnded, int batchSize, Handler<Long> handler) {
    return result -> {
      if (result.succeeded()) {
        if (!listPromise.future().isComplete() && (result.result().size() == batchSize + 1 || streamEnded)) {
          listPromise.complete(result.result());
        } else {
          vertx.setTimer(POLLING_TIME_INTERVAL, handler);
        }
      } else {
        logger.error(result.cause());
        vertx.setTimer(POLLING_TIME_INTERVAL, handler);
      }
    };
  }

  private Future<List<JsonObject>> enrichInstances(List<JsonObject> result, Request request, Context context) {
    Map<String, JsonObject> instances = result.stream()
      .collect(LinkedHashMap::new, (map, instance) -> map.put(instance.getString(INSTANCE_ID_FIELD_NAME), instance), Map::putAll);
    Promise<List<JsonObject>> completePromise = Promise.promise();

    var webClient = WebClient.create(context.owner());
    var httpRequest = webClient.postAbs(request.getOkapiUrl() + INVENTORY_ENRICHED_INSTANCES_ENDPOINT);
    if (request.getOkapiUrl().contains("https:")) {
      httpRequest.ssl(true);
    }
    httpRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpRequest.putHeader(ACCEPT, APPLICATION_JSON);
    httpRequest.putHeader(CONTENT_TYPE, APPLICATION_JSON);

    var enrichedInstancesStream = new BatchStreamWrapper(DATABASE_FETCHING_CHUNK_SIZE);

    //setup pgPool availability checker
    AtomicReference<SimpleConnectionPool<PooledConnection>> queue = new AtomicReference<>();
    try {
      queue.set(getWaitersQueue(PostgresClientFactory.getPool(context.owner(), request.getTenant())));
    } catch (IllegalStateException ex) {
      logger.error(ex.getMessage());
      completePromise.fail(ex);
      return completePromise.future();
    }
    enrichedInstancesStream.setCapacityChecker(() -> queue.get().waiters() > 20);

    JsonObject entries = new JsonObject();
    entries.put(INSTANCE_IDS_ENRICH_PARAM_NAME, new JsonArray(new ArrayList<>(instances.keySet())));
    entries.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS, isSkipSuppressed(request));

    //setup json parser and pipe to write stream
    var responseChecked = new AtomicBoolean();
    var responseCheckAttempts = new AtomicInteger(RESPONSE_CHECK_ATTEMPTS);
    var jsonParser = JsonParser.newParser()
      .objectValueMode();
    jsonParser.pipeTo(enrichedInstancesStream);
    jsonParser.endHandler(e -> closeStreamRelatedObjects(enrichedInstancesStream, webClient, responseChecked, responseCheckAttempts));
    jsonParser.exceptionHandler(throwable -> {
      logger.error("Error has been occurred at JsonParser while reading data from response. Message:{}", throwable.getMessage(),
        throwable);
      closeStreamRelatedObjects(enrichedInstancesStream, webClient, Optional.of(throwable));
      completePromise.fail(throwable);
    });

    httpRequest.as(BodyCodec.jsonStream(jsonParser))
      .sendBuffer(entries.toBuffer())
      .onSuccess(httpResponse -> {
        if (httpResponse.statusCode() != 200) {
          String errorFromStorageMessage = getErrorFromStorageMessage("inventory-storage", request.getOkapiUrl() + INVENTORY_ENRICHED_INSTANCES_ENDPOINT, httpResponse.statusMessage());
          String errorMessage = errorFromStorageMessage + httpResponse.statusCode();
          logger.error(errorMessage);
          responseChecked.set(true);
          completePromise.fail(new IllegalStateException(errorFromStorageMessage));
        }
        responseChecked.set(true);
      })
      .onFailure(e -> {
        logger.error(e.getMessage());
        responseChecked.set(true);
        completePromise.fail(e);
      });

    //set batch handler
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
              logger.info("Instance with instanceId {} wasn't in the request.", instanceId);
            }
          }
        }

        if (enrichedInstancesStream.isTheLastBatch()) {
          if (enrichedInstancesStream.isEndedWithError()) {
            completePromise.tryFail(enrichedInstancesStream.getCause());
          } else {
            completePromise.complete(new ArrayList<>(instances.values()));
          }
        }
      } catch (Exception e) {
        completePromise.fail(e);
      }
    });

    return completePromise.future();
  }

  private void closeStreamRelatedObjects(BatchStreamWrapper databaseWriteStream, WebClient webClient, Optional<Throwable> optional) {
    if (optional.isPresent()) {
      databaseWriteStream.endWithError(optional.get());
    } else {
      databaseWriteStream.end();
    }
    webClient.close();
  }

  private void closeStreamRelatedObjects(BatchStreamWrapper databaseWriteStream, WebClient webClient, AtomicBoolean responseChecked, AtomicInteger attempts) {
    VertxUtils.getVertxFromContextOrNew().setTimer(500, id -> {
      if (responseChecked.get() || attempts.get() == 0) {
        closeStreamRelatedObjects(databaseWriteStream, webClient, Optional.empty());
      } else {
        attempts.decrementAndGet();
        closeStreamRelatedObjects(databaseWriteStream, webClient, responseChecked, attempts);
      }
    });
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
      logger.info("Build records response, instances = {}, instances with srs records = {}.", batch.size(), records.size());
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

    final boolean suppressedRecordsProcessing = getBooleanProperty(request.getRequestId(),
        REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

    List<RecordType> records = new ArrayList<>();
    batch.stream()
      .filter(instance -> {
        final String instanceId = instance.getString(INSTANCE_ID_FIELD_NAME);
        final JsonObject srsInstance = srsResponse.get(instanceId);
        return Objects.nonNull(srsInstance);
      })
      .forEach(instance -> {
        final String instanceId = instance.getString(INSTANCE_ID_FIELD_NAME);
        final JsonObject srsInstance = srsResponse.get(instanceId);
        RecordType record = createRecord(request, srsInstance, instanceId);

        JsonObject updatedSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, instance,
            suppressedRecordsProcessing);
        if (deletedRecordSupport && storageHelper.isRecordMarkAsDeleted(updatedSrsInstance)) {
          record.getHeader()
            .setStatus(StatusType.DELETED);
        }
        String source = storageHelper.getInstanceRecordSource(updatedSrsInstance);
        if (source != null && record.getHeader()
          .getStatus() == null) {
          if (suppressedRecordsProcessing) {
            source = metadataManager.updateMetadataSourceWithDiscoverySuppressedData(source, updatedSrsInstance);
            source = metadataManager.updateElectronicAccessFieldWithDiscoverySuppressedData(source, updatedSrsInstance);
          }
          try {
            record.withMetadata(buildOaiMetadata(request, source));
          } catch (Exception e) {
            logger.error("Error occurred while converting record to xml representation: {}.", e.getMessage(), e);
            logger.debug("Skipping problematic record due the conversion error. Source record id - {}.", storageHelper.getRecordId(srsInstance));
            return;
          }
        }
        if (filterInstance(request, srsInstance)) {
          records.add(record);
        }
      });
    return records;
  }

  private ResumptionTokenType buildResumptionTokenFromRequest(Request request, String requestId, long returnedCount, String nextInstanceId) {
    long cursor = request.getOffset();
    if (nextInstanceId == null) {
      return new ResumptionTokenType()
        .withValue("")
        .withCursor(BigInteger.valueOf(cursor));
    }
    Map<String, String> extraParams = new HashMap<>();
    extraParams.put(OFFSET_PARAM, String.valueOf(cursor + returnedCount));
    extraParams.put(REQUEST_ID_PARAM, requestId);
    extraParams.put(NEXT_RECORD_ID_PARAM, nextInstanceId);
    if (request.getUntil() == null) {
      extraParams.put(UNTIL_PARAM, getUntilDate(request, request.getFrom()));
    }
    int pk = request.getNextInstancePkValue();
    if (pk > 0) {
      extraParams.put(NEXT_INSTANCE_PK_VALUE, String.valueOf(pk));
    }

    String resumptionToken = request.toResumptionToken(extraParams);

    return new ResumptionTokenType()
      .withValue(resumptionToken)
      .withCursor(BigInteger.valueOf(cursor));
  }

  private RecordType createRecord(Request request, JsonObject instance, String instanceId) {
    String identifierPrefix = request.getIdentifierPrefix();
    return new RecordType()
      .withHeader(createHeader(instance, request)
        .withIdentifier(getIdentifier(identifierPrefix, instanceId)));
  }

  private boolean isSkipSuppressed(Request request) {
    return !getBooleanProperty(request.getRequestId(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
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

  private SimpleConnectionPool<PooledConnection> getWaitersQueue(PgPool pgPool) {
    if (Objects.nonNull(pgPool)) {
      try {
        return (SimpleConnectionPool<PooledConnection>) getValueFrom(getValueFrom(pgPool, "pool"), "pool");
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
        logger.error("Cannot save the ids, error from the database: {}.", res.cause().getMessage(), res.cause());
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
        logger.error("Save instance Ids failed: {}.", e.cause().getMessage(), e.cause());
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

  private Future<Map<String, JsonObject>> requestSRSByIdentifiers(SourceStorageSourceRecordsClient srsClient, Vertx vertx,
      List<JsonObject> batch, boolean deletedRecordSupport, int retryAttempts) {
    final List<String> listOfIds = extractListOfIdsForSRSRequest(batch);
    logger.debug("Request to SRS, list id size: {}.", listOfIds.size());
    AtomicInteger attemptsCount = new AtomicInteger(retryAttempts);
    Promise<Map<String, JsonObject>> promise = Promise.promise();
    doPostRequestToSrs(srsClient, vertx, deletedRecordSupport, listOfIds, attemptsCount, retryAttempts, promise);
    return promise.future();
  }

  private void doPostRequestToSrs(SourceStorageSourceRecordsClient srsClient, Vertx vertx, boolean deletedRecordSupport,
      List<String> listOfIds, AtomicInteger attemptsCount, int retryAttempts, Promise<Map<String, JsonObject>> promise) {
    try {
      srsClient.postSourceStorageSourceRecords("INSTANCE", null, deletedRecordSupport, listOfIds, asyncResult -> {
        if (asyncResult.succeeded()) {
          HttpResponse<Buffer> srsResponse = asyncResult.result();
          int statusCode = srsResponse.statusCode();
          String statusMessage = srsResponse.statusMessage();
          if (statusCode >= 400) {
            retrySRSRequest(srsClient, vertx, deletedRecordSupport, listOfIds, attemptsCount, retryAttempts, promise, statusCode,
                statusMessage);
            return;
          }
          if (statusCode != 200) {
            String errorMsg = getErrorFromStorageMessage("source-record-storage", "/source-storage/source-records", srsResponse.statusMessage());
            handleException(promise, new IllegalStateException(errorMsg));
            return;
          }
          handleSrsResponse(promise, srsResponse.body());
        } else {
          logger.error("Error has been occurred while requesting the SRS: {}.", asyncResult.cause().getMessage(), asyncResult.cause());
            retrySRSRequest(srsClient, vertx, deletedRecordSupport, listOfIds, attemptsCount, retryAttempts, promise, -1, "");
        }
      });
    } catch (Exception e) {
      handleException(promise, e);
    }
  }

  private void retrySRSRequest(SourceStorageSourceRecordsClient srsClient, Vertx vertx, boolean deletedRecordSupport, List<String> listOfIds, AtomicInteger attemptsCount, int retryAttempts, Promise<Map<String, JsonObject>> promise, int statusCode, String statusMessage) {
    if (statusCode > 0) {
      logger.debug("Got error response form SRS, status code: {}, status message: {}.", statusCode, statusMessage);
    } else {
      logger.debug("Error has been occurred while requesting SRS.");
    }
    if (attemptsCount.decrementAndGet() <= 0) {
      String errorMessage = "SRS didn't respond with expected status code after " + retryAttempts
          + " attempts. Canceling further request processing.";
      handleException(promise, new IllegalStateException(errorMessage));
      return;
    }
    logger.debug("Trying to request SRS again, attempts left: {}.", attemptsCount.get());
    vertx.setTimer(REREQUEST_SRS_DELAY, timer -> doPostRequestToSrs(srsClient, vertx, deletedRecordSupport, listOfIds, attemptsCount, retryAttempts, promise));
  }

  private void handleSrsResponse(Promise<Map<String, JsonObject>> promise, Buffer buffer) {
    final Map<String, JsonObject> result = Maps.newHashMap();
    try {
      final Object jsonResponse = buffer.toJson();
      if (jsonResponse instanceof JsonObject) {
        JsonObject entries = (JsonObject) jsonResponse;
        final JsonArray records = entries.getJsonArray("sourceRecords");
        records.stream()
          .filter(Objects::nonNull)
          .map(JsonObject.class::cast)
          .forEach(jo -> result.put(jo.getJsonObject("externalIdsHolder")
            .getString(INSTANCE_ID_FIELD_NAME), jo));
      } else {
        logger.debug("Can't process SRS response: {}.", buffer);
      }
      promise.complete(result);
    } catch (DecodeException ex) {
      String msg = "Invalid json has been returned from SRS, cannot parse response to json.";
      handleException(promise, new IllegalStateException(msg, ex));
    } catch (Exception e) {
      handleException(promise, e);
    }
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
