package org.folio.oaipmh.processors;

import com.google.common.collect.Maps;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.impl.HttpRequestImpl;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Tuple;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.helpers.AbstractHelper;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.service.InstancesService;
import org.folio.rest.client.SourceStorageSourceRecordsClient;
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

import javax.ws.rs.core.Response;
import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.EXPIRATION_DATE_RESUMPTION_TOKEN_PARAM;
import static org.folio.oaipmh.Constants.LOCATION;
import static org.folio.oaipmh.Constants.NEXT_INSTANCE_PK_VALUE;
import static org.folio.oaipmh.Constants.NEXT_RECORD_ID_PARAM;
import static org.folio.oaipmh.Constants.OFFSET_PARAM;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_SRS_HTTP_REQUEST_RETRY_ATTEMPTS;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.REQUEST_ID_PARAM;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_TIMEOUT;
import static org.folio.oaipmh.Constants.RETRY_ATTEMPTS;
import static org.folio.oaipmh.Constants.STATUS_CODE;
import static org.folio.oaipmh.Constants.STATUS_MESSAGE;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.CALL_NUMBER;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.ITEMS;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.NAME;

public class MarcWithHoldingsRequestHelper extends AbstractHelper {

  private static final int DATABASE_FETCHING_CHUNK_SIZE = 5000;

  private static final String INSTANCE_ID_FIELD_NAME = "instanceId";

  private static final String SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS = "skipSuppressedFromDiscoveryRecords";

  private static final String INSTANCE_IDS_ENRICH_PARAM_NAME = "instanceIds";

  private static final String DELETED_RECORD_SUPPORT_PARAM_NAME = "deletedRecordSupport";

  private static final String START_DATE_PARAM_NAME = "startDate";

  private static final String END_DATE_PARAM_NAME = "endDate";

  private static final String TEMPORARY_LOCATION = "temporaryLocation";

  private static final String PERMANENT_LOCATION = "permanentLocation";

  private static final String EFFECTIVE_LOCATION = "effectiveLocation";

  private static final String CODE = "code";

  private static final String HOLDINGS = "holdings";

  private static final String INVENTORY_ITEMS_AND_HOLDINGS_ENDPOINT = "/inventory-hierarchy/items-and-holdings";

  private static final String INVENTORY_UPDATED_INSTANCES_ENDPOINT = "/inventory-hierarchy/updated-instance-ids";

  private static final String ERROR_FROM_STORAGE = "Got error response from %s, uri: '%s' message: %s";
  public static final int RESPONSE_CHECK_ATTEMPTS = 30;

  protected final Logger logger = LogManager.getLogger(getClass());

  public static final MarcWithHoldingsRequestHelper INSTANCE = new MarcWithHoldingsRequestHelper();
  private final Vertx vertx;

  private static final int REREQUEST_SRS_DELAY = 2000;

  private static final int POLLING_TIME_INTERVAL = 500;
  private static final int MAX_WAIT_UNTIL_TIMEOUT = 1000 * 60 * 20;
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
      RequestMetadataLb requestMetadata = new RequestMetadataLb().setLastUpdatedDate(lastUpdateDate);

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
          saveInstancesExecutor.executeBlocking(downloadInstancesPromise -> downloadInstances(request, oaipmhResponsePromise,
              downloadInstancesPromise, downloadContext, requestId), downloadInstancesResult -> {
                instancesService.updateRequestStreamEnded(requestId, true, request.getTenant());
                if (downloadInstancesResult.succeeded()) {
                  logger.info("Downloading instances complete.");
                } else {
                  logger.error("Downloading instances was canceled due to the error. ", downloadInstancesResult.cause());
                  if (!oaipmhResponsePromise.future()
                    .isComplete()) {
                    oaipmhResponsePromise.fail(new IllegalStateException(downloadInstancesResult.cause()));
                  }
                }
              });
        }
      })
        .onFailure(th -> handleException(oaipmhResponsePromise, th));
    } catch (Exception e) {
      handleException(oaipmhResponsePromise, e);
    }
    return oaipmhResponsePromise.future();
  }

  private void processBatch(Request request, Context context, Promise<Response> oaiPmhResponsePromise, String requestId,
      boolean firstBatch) {
    try {
      boolean deletedRecordSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request.getRequestId());
      int batchSize = Integer
        .parseInt(RepositoryConfigurationUtil.getProperty(request.getRequestId(), REPOSITORY_MAX_RECORDS_PER_RESPONSE));

      getNextInstances(request, batchSize, requestId, firstBatch).future()
        .onComplete(fut -> {
          if (fut.failed()) {
            logger.error("Get instances failed: {}.", fut.cause()
              .getMessage(), fut.cause());
            oaiPmhResponsePromise.fail(fut.cause());
            return;
          }

          List<JsonObject> instances = fut.result();
          logger.debug("Processing instances: {}.", instances.size());
          if (CollectionUtils.isEmpty(instances) && !firstBatch) {
            handleException(oaiPmhResponsePromise, new IllegalArgumentException("Specified resumption token doesn't exists."));
            return;
          }

          if (!firstBatch && (CollectionUtils.isNotEmpty(instances) && !instances.get(0)
            .getString(INSTANCE_ID_FIELD_NAME)
            .equals(request.getNextRecordId()))) {
            handleException(oaiPmhResponsePromise, new IllegalArgumentException("Stale resumption token."));
            return;
          }

          if (CollectionUtils.isEmpty(instances)) {
            logger.debug("Got empty instances.");
            buildRecordsResponse(request, requestId, instances, new HashMap<>(), firstBatch, null, deletedRecordSupport)
              .onSuccess(oaiPmhResponsePromise::complete)
              .onFailure(e -> handleException(oaiPmhResponsePromise, e));
            return;
          }

          String nextInstanceId = instances.size() <= batchSize ? null
              : instances.get(batchSize)
                .getString(INSTANCE_ID_FIELD_NAME);
          List<JsonObject> instancesWithoutLast = nextInstanceId != null ? instances.subList(0, batchSize) : instances;
          final SourceStorageSourceRecordsClient srsClient = createAndSetupSrsClient(request);

          int retryAttempts = Integer
            .parseInt(RepositoryConfigurationUtil.getProperty(request.getRequestId(), REPOSITORY_SRS_HTTP_REQUEST_RETRY_ATTEMPTS));
          requestSRSByIdentifiers(srsClient, context.owner(), instancesWithoutLast, deletedRecordSupport, retryAttempts)
            .onSuccess(res -> buildRecordsResponse(request, requestId, instancesWithoutLast, res, firstBatch, nextInstanceId,
                deletedRecordSupport).onSuccess(oaiPmhResponsePromise::complete)
                  .onFailure(e -> handleException(oaiPmhResponsePromise, e)))
            .onFailure(e -> handleException(oaiPmhResponsePromise, e));
        });
    } catch (Exception e) {
      handleException(oaiPmhResponsePromise, e);
    }
  }

  private SourceStorageSourceRecordsClient createAndSetupSrsClient(Request request) {
    return new SourceStorageSourceRecordsClient(request.getOkapiUrl(), request.getTenant(), request.getOkapiToken(),
        WebClientProvider.getWebClientForSRSByTenant(request.getTenant(), request.getRequestId()));
  }

  private void downloadInstances(Request request, Promise<Response> oaiPmhResponsePromise, Promise<Object> downloadInstancesPromise,
                                 Context vertxContext, String requestId) {

    HttpRequestImpl<Buffer> httpRequest = (HttpRequestImpl<Buffer>) buildInventoryQuery(request);
    PostgresClient postgresClient = PostgresClient.getInstance(vertxContext.owner(), request.getTenant());
    setupBatchHttpStream(oaiPmhResponsePromise, httpRequest, request.getTenant(), requestId, postgresClient, downloadInstancesPromise);
  }

  private void setupBatchHttpStream(Promise<?> promise, HttpRequestImpl<Buffer> inventoryHttpRequest,
                                    String tenant, String requestId, PostgresClient postgresClient, Promise<Object> downloadInstancesPromise ) {
    var jsonParser = JsonParser.newParser()
      .objectValueMode();
    var batch = new ArrayList<JsonEvent>();
    jsonParser.handler(event -> {
      batch.add(event);
      if (batch.size() >= DATABASE_FETCHING_CHUNK_SIZE) {
        saveInstancesIds(new ArrayList<>(batch), tenant, requestId,  postgresClient);
        batch.clear();
      }
    });
    jsonParser.endHandler(e -> {
      if (!batch.isEmpty()) {
        saveInstancesIds(new ArrayList<>(batch), tenant, requestId, postgresClient);
        batch.clear();
      }
      downloadInstancesPromise.complete();
    });
    jsonParser.exceptionHandler(throwable -> {
      logger.error("Error has been occurred at JsonParser while reading data from response while setting up batch http stream. Message: {}", throwable.getMessage(),
          throwable);
      downloadInstancesPromise.complete(throwable);
      promise.fail(throwable);
    });

    inventoryHttpRequest.as(BodyCodec.jsonStream(jsonParser))
      .send()
      .onSuccess(resp -> {
        if (resp.statusCode() != 200) {
          String errorMsg = getErrorFromStorageMessage("inventory-storage", inventoryHttpRequest.uri(), resp.statusMessage());
          promise.fail(new IllegalStateException(errorMsg));
        }
        downloadInstancesPromise.tryComplete();
      })
      .onFailure(throwable -> {
        logger.error("Error has been occurred on failure of inventory http request. Message: {}", throwable.getMessage(),
          throwable);
        promise.fail(throwable);
      });
  }

  private HttpRequest<Buffer> buildInventoryQuery(Request request) {
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
        String.valueOf(RepositoryConfigurationUtil.isDeletedRecordsEnabled(request.getRequestId())));
    paramMap.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS, String.valueOf(isSkipSuppressed(request)));

    final String params = paramMap.entrySet()
      .stream()
      .map(e -> e.getKey() + "=" + e.getValue())
      .collect(Collectors.joining("&"));

    String inventoryQuery = format("%s%s?%s", request.getOkapiUrl(), INVENTORY_UPDATED_INSTANCES_ENDPOINT, params);

    logger.debug("Sending request to {}", inventoryQuery);
    final HttpRequest<Buffer> httpRequest = WebClientProvider.getWebClientToDownloadInstances()
      .getAbs(inventoryQuery);
    httpRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpRequest.putHeader(ACCEPT, APPLICATION_JSON);
    if (request.getOkapiUrl()
      .contains("https")) {
      httpRequest.ssl(true);
    }
    return httpRequest;
  }

  private Promise<List<JsonObject>> getNextInstances(Request request, int batchSize, String requestId,
      boolean firstBatch) {
    Promise<List<JsonObject>> promise = Promise.promise();
    final Promise<List<Instances>> listPromise = Promise.promise();
    AtomicInteger retryCount = new AtomicInteger();
    vertx.setTimer(200, id -> getNextBatch(requestId, request, firstBatch, batchSize, listPromise, retryCount));
    listPromise.future()
      .compose(instances -> {
        if (CollectionUtils.isNotEmpty(instances)) {
          List<JsonObject> jsonInstances = instances.stream()
            .map(Instances::getJson)
            .map(JsonObject::new)
            .collect(Collectors.toList());
          if (instances.size() > batchSize) {
            request.setNextInstancePkValue(instances.get(batchSize)
              .getId());
          }
          return enrichInstances(jsonInstances, request);
        }
        logger.debug("Skipping enrich instances call, empty instance ids list returned.");
        return Future.succeededFuture(Collections.emptyList());
      })
      .onSuccess(promise::complete)
      .onFailure(throwable -> {
        logger.error("Cannot get batch of instances ids from database: {}.", throwable.getMessage(), throwable);
        promise.fail(throwable);
      });

    return promise;
  }

  private void getNextBatch(String requestId, Request request, boolean firstBatch, int batchSize,
      Promise<List<Instances>> listPromise, AtomicInteger retryCount) {
    if (retryCount.incrementAndGet() > MAX_POLLING_ATTEMPTS) {
      listPromise.fail(new IllegalStateException(
          "The instance list is empty after " + retryCount.get() + " attempts. Stop polling and return fail response."));
      return;
    }
    instancesService.getRequestMetadataByRequestId(requestId, request.getTenant())
      .compose(requestMetadata -> Future.succeededFuture(requestMetadata.getStreamEnded()))
      .compose(streamEnded -> {
        if (firstBatch) {
          return instancesService.getInstancesList(batchSize + 1, requestId, request.getTenant())
            .onComplete(handleInstancesDbResponse(listPromise, streamEnded, batchSize,
                timer -> getNextBatch(requestId, request, firstBatch, batchSize, listPromise, retryCount)));
        }
        int autoIncrementedId = request.getNextInstancePkValue();
        return instancesService.getInstancesList(batchSize + 1, requestId, autoIncrementedId, request.getTenant())
          .onComplete(handleInstancesDbResponse(listPromise, streamEnded, batchSize,
              timer -> getNextBatch(requestId, request, firstBatch, batchSize, listPromise, retryCount)));
      });
  }

  private Handler<AsyncResult<List<Instances>>> handleInstancesDbResponse(Promise<List<Instances>> listPromise, boolean streamEnded,
      int batchSize, Handler<Long> handler) {
    return result -> {
      if (result.succeeded()) {
        if (!listPromise.future()
          .isComplete()
            && (result.result()
              .size() == batchSize + 1 || streamEnded)) {
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

  private Future<List<JsonObject>> enrichInstances(List<JsonObject> result, Request request) {
    Map<String, JsonObject> instances = result.stream()
      .collect(LinkedHashMap::new, (map, instance) -> map.put(instance.getString(INSTANCE_ID_FIELD_NAME), instance), Map::putAll);
    Promise<List<JsonObject>> completePromise = Promise.promise();
    var webClient = WebClientProvider.getWebClient();
    var httpRequest = webClient.postAbs(request.getOkapiUrl() + INVENTORY_ITEMS_AND_HOLDINGS_ENDPOINT);
    if (request.getOkapiUrl()
      .contains("https:")) {
      httpRequest.ssl(true);
    }
    httpRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpRequest.putHeader(ACCEPT, APPLICATION_JSON);
    httpRequest.putHeader(CONTENT_TYPE, APPLICATION_JSON);

    JsonObject entries = new JsonObject();
    entries.put(INSTANCE_IDS_ENRICH_PARAM_NAME, new JsonArray(new ArrayList<>(instances.keySet())));
    entries.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS, isSkipSuppressed(request));

    var jsonParser = JsonParser.newParser()
      .objectValueMode();
    jsonParser.handler(event -> {
      JsonObject itemsAndHoldingsFields = event.objectValue();
      String instanceId = itemsAndHoldingsFields.getString(INSTANCE_ID_FIELD_NAME);
      JsonObject instance = instances.get(instanceId);
      if (instance != null) {
        enrichDiscoverySuppressed(itemsAndHoldingsFields, instance);
        instance.put(RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS, itemsAndHoldingsFields);
        // case when no items
        if (itemsAndHoldingsFields.getJsonArray(ITEMS)
          .isEmpty()) {
          enrichOnlyEffectiveLocationEffectiveCallNumberFromHoldings(instance);
        } else {
          adjustItems(instance);
        }
      } else {
        logger.info("Instance with instanceId {} wasn't in the request.", instanceId);
      }
    });
    jsonParser.exceptionHandler(throwable -> {
      logger.error("Error has been occurred at JsonParser while reading data from response while enriching instances. Message:{}", throwable.getMessage(),
          throwable);
      completePromise.fail(throwable);
    });

    httpRequest.as(BodyCodec.jsonStream(jsonParser))
      .sendBuffer(entries.toBuffer())
      .onSuccess(httpResponse -> {
        logger.info("Response for items and holdings {}: {}", httpResponse.statusCode(), httpResponse.statusMessage());
        if (httpResponse.statusCode() != 200) {
          String errorFromStorageMessage = getErrorFromStorageMessage("inventory-storage",
              request.getOkapiUrl() + INVENTORY_ITEMS_AND_HOLDINGS_ENDPOINT, httpResponse.statusMessage());
          String errorMessage = errorFromStorageMessage + httpResponse.statusCode();
          logger.error(errorMessage);
          completePromise.fail(new IllegalStateException(errorFromStorageMessage));
        }
        completePromise.complete(new ArrayList<>(instances.values()));
      })
      .onFailure(e -> {
        logger.error(e.getMessage());
        completePromise.fail(e);
      });
    return completePromise.future();
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

  private void enrichOnlyEffectiveLocationEffectiveCallNumberFromHoldings(JsonObject instance) {
    JsonArray holdingsJson = instance.getJsonObject(ITEMS_AND_HOLDINGS_FIELDS)
      .getJsonArray(HOLDINGS);
    JsonArray itemsJson = instance.getJsonObject(ITEMS_AND_HOLDINGS_FIELDS)
      .getJsonArray(ITEMS);
    for (Object holding : holdingsJson) {
      if (holding instanceof JsonObject) {
        JsonObject holdingJson = (JsonObject) holding;
        JsonObject callNumberJson = holdingJson.getJsonObject(CALL_NUMBER);
        JsonObject locationJson = holdingJson.getJsonObject(LOCATION);
        JsonObject effectiveLocationJson = locationJson.getJsonObject(EFFECTIVE_LOCATION);
        JsonObject itemJson = new JsonObject();
        itemJson.put(CALL_NUMBER, callNumberJson);
        JsonObject locationItemJson = new JsonObject();
        locationItemJson.put(NAME, effectiveLocationJson.getString(NAME));
        effectiveLocationJson.remove(NAME);
        locationItemJson.put(LOCATION, effectiveLocationJson);
        itemJson.put(LOCATION, locationItemJson);
        itemsJson.add(itemJson);
      }
    }
  }

  private void adjustItems(JsonObject instance) {
    JsonArray itemsJson = instance.getJsonObject(ITEMS_AND_HOLDINGS_FIELDS)
      .getJsonArray(ITEMS);
    for (Object item: itemsJson) {
      JsonObject itemJson = (JsonObject) item;
      itemJson.getJsonObject(LOCATION).put(NAME, itemJson.getJsonObject(LOCATION).getJsonObject(LOCATION).getString(NAME));
      itemJson.getJsonObject(LOCATION).getJsonObject(LOCATION).remove(NAME);
      itemJson.getJsonObject(LOCATION).getJsonObject(LOCATION).remove(CODE);
      itemJson.getJsonObject(LOCATION).remove(TEMPORARY_LOCATION);
      itemJson.getJsonObject(LOCATION).remove(PERMANENT_LOCATION);
    }
  }

  private Future<Response> buildRecordsResponse(Request request, String requestId, List<JsonObject> batch,
      Map<String, JsonObject> srsResponse, boolean firstBatch, String nextInstanceId, boolean deletedRecordSupport) {

    Promise<Response> promise = Promise.promise();
    try {
      List<RecordType> records = buildRecordsList(request, batch, srsResponse, deletedRecordSupport);
      logger.debug("Build records response, instances = {}, instances with srs records = {}.", batch.size(), records.size());
      ResponseHelper responseHelper = getResponseHelper();
      OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request);
      if (records.isEmpty() && nextInstanceId == null && firstBatch) {
        oaipmh.withErrors(createNoRecordsFoundError());
      } else {
        oaipmh.withListRecords(new ListRecordsType().withRecords(records));
      }
      Response response;
      if (oaipmh.getErrors()
        .isEmpty()) {
        if (!firstBatch || nextInstanceId != null) {
          ResumptionTokenType resumptionToken = buildResumptionTokenFromRequest(request, requestId, records.size(), nextInstanceId);
          oaipmh.getListRecords()
            .withResumptionToken(resumptionToken);
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

        JsonObject updatedSrsWithItemsData = metadataManager.populateMetadataWithItemsData(srsInstance, instance,
            suppressedRecordsProcessing);
        JsonObject updatedSrsInstance = metadataManager.populateMetadataWithHoldingsData(updatedSrsWithItemsData, instance,
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
            logger.debug("Skipping problematic record due the conversion error. Source record id - {}.",
                storageHelper.getRecordId(srsInstance));
            return;
          }
        }
        if (filterInstance(request, srsInstance)) {
          records.add(record);
        }
      });
    return records;
  }

  private ResumptionTokenType buildResumptionTokenFromRequest(Request request, String requestId, long returnedCount,
      String nextInstanceId) {
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
    extraParams.put(EXPIRATION_DATE_RESUMPTION_TOKEN_PARAM, String.valueOf(Instant.now().with(ChronoField.NANO_OF_SECOND, 0).plusSeconds(RESUMPTION_TOKEN_TIMEOUT)));
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
      .withExpirationDate(Instant.now().with(ChronoField.NANO_OF_SECOND, 0).plusSeconds(RESUMPTION_TOKEN_TIMEOUT))
      .withCursor(BigInteger.valueOf(cursor));
  }

  private RecordType createRecord(Request request, JsonObject instance, String instanceId) {
    String identifierPrefix = request.getIdentifierPrefix();
    return new RecordType().withHeader(createHeader(instance, request).withIdentifier(getIdentifier(identifierPrefix, instanceId)));
  }

  private boolean isSkipSuppressed(Request request) {
    return !getBooleanProperty(request.getRequestId(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
  }

  private Promise<Void> saveInstancesIds(List<JsonEvent> instances, String tenant, String requestId,
                                         PostgresClient postgresClient) {
    Promise<Void> promise = Promise.promise();
    List<Instances> instancesList = toInstancesList(instances, UUID.fromString(requestId));
    saveInstances(instancesList, tenant, requestId, postgresClient).onComplete(res -> {
      if (res.failed()) {
        logger.error("Cannot save the ids, error from the database: {}.", res.cause()
          .getMessage(), res.cause());
        promise.fail(res.cause());
      } else {
        promise.complete();
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

      String sql = "INSERT INTO " + PostgresClient.convertToPsqlStandard(tenantId)
          + ".instances (instance_id, request_id, json) VALUES ($1, $2, $3) RETURNING instance_id";

      if (e.failed()) {
        logger.error("Save instance Ids failed: {}.", e.cause()
          .getMessage(), e.cause());
        promise.fail(e.cause());
      } else {
        PgConnection connection = e.result();
        connection.preparedQuery(sql)
          .executeBatch(batch, queryRes -> {
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
    return jsonEventInstances.stream()
      .map(JsonEvent::objectValue)
      .map(inst -> new Instances().setInstanceId(UUID.fromString(inst.getString(INSTANCE_ID_FIELD_NAME)))
        .setJson(inst.toString())
        .setRequestId(requestId))
      .collect(Collectors.toList());
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
        Map <String, String> retrySRSRequestParams = new HashMap<>();
        if (asyncResult.succeeded()) {
          HttpResponse<Buffer> srsResponse = asyncResult.result();
          int statusCode = srsResponse.statusCode();
          String statusMessage = srsResponse.statusMessage();
          if (statusCode >= 400) {
            retrySRSRequestParams.put(RETRY_ATTEMPTS, String.valueOf(retryAttempts));
            retrySRSRequestParams.put(STATUS_CODE, String.valueOf(statusCode));
            retrySRSRequestParams.put(STATUS_MESSAGE, statusMessage);
            retrySRSRequest(srsClient, vertx, deletedRecordSupport, listOfIds, attemptsCount, promise, retrySRSRequestParams);
            return;
          }
          if (statusCode != 200) {
            String errorMsg = getErrorFromStorageMessage("source-record-storage", "/source-storage/source-records",
                srsResponse.statusMessage());
            handleException(promise, new IllegalStateException(errorMsg));
            return;
          }
          handleSrsResponse(promise, srsResponse.body());
        } else {
          logger.error("Error has been occurred while requesting the SRS: {}.", asyncResult.cause()
            .getMessage(), asyncResult.cause());
          retrySRSRequestParams.put(RETRY_ATTEMPTS, String.valueOf(retryAttempts));
          retrySRSRequestParams.put(STATUS_CODE, String.valueOf(-1));
          retrySRSRequestParams.put(STATUS_MESSAGE, "");
          retrySRSRequest(srsClient, vertx, deletedRecordSupport, listOfIds, attemptsCount, promise, retrySRSRequestParams);
        }
      });
    } catch (Exception e) {
      handleException(promise, e);
    }
  }

  private void retrySRSRequest(SourceStorageSourceRecordsClient srsClient, Vertx vertx, boolean deletedRecordSupport,
      List<String> listOfIds, AtomicInteger attemptsCount, Promise<Map<String, JsonObject>> promise, Map <String, String> retrySRSRequestParams) {
    if (Integer.parseInt(retrySRSRequestParams.get(STATUS_CODE)) > 0) {
      logger.debug("Got error response form SRS, status code: {}, status message: {}.",
        Integer.parseInt(retrySRSRequestParams.get(STATUS_CODE)), retrySRSRequestParams.get(STATUS_MESSAGE));
    } else {
      logger.debug("Error has been occurred while requesting SRS.");
    }
    if (attemptsCount.decrementAndGet() <= 0) {
      String errorMessage = "SRS didn't respond with expected status code after " + Integer.parseInt(retrySRSRequestParams.get(RETRY_ATTEMPTS))
          + " attempts. Canceling further request processing.";
      handleException(promise, new IllegalStateException(errorMessage));
      return;
    }
    logger.debug("Trying to request SRS again, attempts left: {}.", attemptsCount.get());
    vertx.setTimer(REREQUEST_SRS_DELAY,
        timer -> doPostRequestToSrs(srsClient, vertx, deletedRecordSupport, listOfIds, attemptsCount,
          Integer.parseInt(retrySRSRequestParams.get(RETRY_ATTEMPTS)), promise));
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

    return batch.stream()
      .filter(Objects::nonNull)
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
