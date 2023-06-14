package org.folio.oaipmh.processors;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.EXPIRATION_DATE_RESUMPTION_TOKEN_PARAM;
import static org.folio.oaipmh.Constants.INSTANCE_ID_FIELD_NAME;
import static org.folio.oaipmh.Constants.INVENTORY;
import static org.folio.oaipmh.Constants.INVENTORY_STORAGE;
import static org.folio.oaipmh.Constants.NEXT_INSTANCE_PK_VALUE;
import static org.folio.oaipmh.Constants.NEXT_RECORD_ID_PARAM;
import static org.folio.oaipmh.Constants.OFFSET_PARAM;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.REPOSITORY_FETCHING_CHUNK_SIZE;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_RECORDS_SOURCE;
import static org.folio.oaipmh.Constants.REPOSITORY_SRS_HTTP_REQUEST_RETRY_ATTEMPTS;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.REQUEST_COMPLETE_LIST_SIZE_PARAM;
import static org.folio.oaipmh.Constants.REQUEST_ID_PARAM;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_TIMEOUT;
import static org.folio.oaipmh.Constants.RETRY_ATTEMPTS;
import static org.folio.oaipmh.Constants.SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS;
import static org.folio.oaipmh.Constants.SRS;
import static org.folio.oaipmh.Constants.STATUS_CODE;
import static org.folio.oaipmh.Constants.STATUS_MESSAGE;
import static org.folio.oaipmh.Constants.SUPPRESS_FROM_DISCOVERY;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getProperty;
import static org.folio.oaipmh.service.MetricsCollectingService.MetricOperation.INSTANCES_PROCESSING;
import static org.folio.oaipmh.service.MetricsCollectingService.MetricOperation.SEND_REQUEST;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

import com.google.common.collect.Maps;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Tuple;
import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.domain.StatisticsHolder;
import org.folio.oaipmh.helpers.AbstractGetRecordsHelper;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.service.InstancesService;
import org.folio.oaipmh.service.MetricsCollectingService;
import org.folio.oaipmh.service.SourceStorageSourceRecordsClientWrapper;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.spring.SpringContextUtil;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.ListIdentifiersType;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.VerbType;
import org.springframework.beans.factory.annotation.Autowired;

public class GetListRecordsRequestHelper extends AbstractGetRecordsHelper {

  protected final Logger logger = LogManager.getLogger(getClass());

  private static final String DELETED_RECORD_SUPPORT_PARAM_NAME = "deletedRecordSupport";
  private static final String ONLY_INSTANCE_UPDATE_DATE = "onlyInstanceUpdateDate";
  public static final String FOLIO_RECORD_SOURCE = "source";

  private static final String START_DATE_PARAM_NAME = "startDate";
  private static final String END_DATE_PARAM_NAME = "endDate";

  private static final String INVENTORY_UPDATED_INSTANCES_ENDPOINT = "/inventory-hierarchy/updated-instance-ids";

  private static final String DOWNLOAD_INSTANCES_MISSED_PERMISSION = "Cannot download instances due to lack of permission, permission required - inventory-storage.inventory-hierarchy.updated-instances-ids.collection.get";

  private static final int REREQUEST_SRS_DELAY = 2000;
  private static final int POLLING_TIME_INTERVAL = 500;
  private static final int MAX_WAIT_UNTIL_TIMEOUT = 1000 * 60 * 20;
  private static final int MAX_POLLING_ATTEMPTS = MAX_WAIT_UNTIL_TIMEOUT / POLLING_TIME_INTERVAL;
  private static final long MAX_EVENT_LOOP_EXECUTE_TIME_NS = 60_000_000_000L;
  private static final int MAX_RECORDS_PER_REQUEST_FROM_INVENTORY = 50;

  public static final int TRACKER_LIMIT = 15;

  public static final GetListRecordsRequestHelper INSTANCE = new GetListRecordsRequestHelper();

  private final Vertx vertx;
  private final WorkerExecutor saveInstancesExecutor;
  private final Context downloadContext;

  private final AtomicInteger batchesSizeCounter = new AtomicInteger();

  private final MetricsCollectingService metricsCollectingService = MetricsCollectingService.getInstance();
  private InstancesService instancesService;

  private SourceStorageSourceRecordsClientWrapper srsClient;

  public static GetListRecordsRequestHelper getInstance() {
    return INSTANCE;
  }

  private GetListRecordsRequestHelper() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    var vertxOptions = new VertxOptions();
    vertxOptions.setMaxEventLoopExecuteTime(MAX_EVENT_LOOP_EXECUTE_TIME_NS);
    vertx = Vertx.vertx(vertxOptions);
    downloadContext = vertx.getOrCreateContext();
    saveInstancesExecutor = vertx.createSharedWorkerExecutor("saving-executor", 5);
  }

  /**
   * Handle MarcWithHoldings request
   */
  @Override
  public Future<Response> handle(Request request, Context vertxContext) {
    Promise<Response> oaipmhResponsePromise = Promise.promise();
    metricsCollectingService.startMetric(request.getRequestId(), SEND_REQUEST);
    try {
      String resumptionToken = request.getResumptionToken();
      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, oaipmhResponsePromise, errors);
      }

      var requestId = request.getRequestId();
      OffsetDateTime lastUpdateDate = OffsetDateTime.now(ZoneId.systemDefault());
      RequestMetadataLb requestMetadata = new RequestMetadataLb().setLastUpdatedDate(lastUpdateDate);

      Future<RequestMetadataLb> updateRequestMetadataFuture;
      if (resumptionToken == null) {
        requestMetadata.setRequestId(UUID.fromString(requestId));
        if (request.getCursor() == 0) {
          requestMetadata.setStartedDate(lastUpdateDate);
          updateRequestMetadataFuture = instancesService.saveRequestMetadata(requestMetadata, request.getTenant());
        } else {
          updateRequestMetadataFuture = errorsService.saveErrorsAndUpdateRequestMetadata(request.getTenant(), requestId, requestMetadata);
        }
      } else {
        updateRequestMetadataFuture = Future.succeededFuture();
      }
      var batchInstancesStatistics = new StatisticsHolder();
      updateRequestMetadataFuture.onSuccess(res -> {
        boolean isFirstBatch = resumptionToken == null;
        processBatch(request, vertxContext, oaipmhResponsePromise, requestId, isFirstBatch, batchInstancesStatistics, lastUpdateDate);
        if (isFirstBatch) {
          var downloadInstancesStatistics = new StatisticsHolder();
          saveInstancesExecutor.executeBlocking(downloadInstancesPromise -> downloadInstances(request, oaipmhResponsePromise,
              downloadInstancesPromise, downloadContext, downloadInstancesStatistics), downloadInstancesResult -> {
              updateRequestStreamEnded(requestId, request.getTenant(), downloadInstancesStatistics);
                if (downloadInstancesResult.succeeded()) {
                  logger.info("handle:: Downloading instances complete for requestId {}", request.getRequestId());
                } else {
                  logger.error("handle:: Downloading instances was canceled for requestId {} due to the error {}", request.getRequestId(), downloadInstancesResult.cause().getMessage());
                  if (!oaipmhResponsePromise.future().isComplete()) {
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
    return oaipmhResponsePromise.future().onComplete(responseAsyncResult -> metricsCollectingService.endMetric(request.getRequestId(), SEND_REQUEST));
  }

  private void updateRequestStreamEnded(String requestId, String tenantId, StatisticsHolder downloadInstancesStatistics) {
    Promise<Void> promise = Promise.promise();
    PostgresClient.getInstance(downloadContext.owner(), tenantId).withTrans(connection -> {
      Tuple params = Tuple.of(true, UUID.fromString(requestId), downloadInstancesStatistics.getDownloadedAndSavedInstancesCounter(), downloadInstancesStatistics.getFailedToSaveInstancesCounter());
      String updateRequestMetadataSql = "UPDATE " + PostgresClient.convertToPsqlStandard(tenantId)
        + ".request_metadata_lb SET stream_ended = $1, downloaded_and_saved_instances_counter = $3, failed_to_save_instances_counter = $4 WHERE request_id = $2";

      List<Tuple> batch = new ArrayList<>();
      downloadInstancesStatistics.getFailedToSaveInstancesIds()
        .forEach(instanceId -> batch.add(Tuple.of(UUID.fromString(requestId), UUID.fromString(instanceId))));
      String sql = "INSERT INTO " + PostgresClient.convertToPsqlStandard(tenantId)
        + ".failed_to_save_instances_ids (request_id, instance_id) VALUES ($1, $2)";

      connection.execute(updateRequestMetadataSql, params)
        .compose(x -> connection.execute(sql, batch))
        .onComplete(result -> {
          connection.getPgConnection().close();
          if (result.failed()) {
            var error = result.cause();
            logger.error("updateRequestStreamEnded:: For requestId {} error updating request metadata on instances stream completion {}", requestId,  error);
            promise.fail(error);
          } else {
            logger.info("updateRequestStreamEnded:: For requestId {} updating request metadata on instances stream completion finished", requestId);
            promise.complete();
          }
        });
      return Future.succeededFuture();
    });
  }

  private void processBatch(Request request, Context context, Promise<Response> oaiPmhResponsePromise, String requestId,
                            boolean firstBatch, StatisticsHolder statistics, OffsetDateTime lastUpdateDate) {
    try {
      boolean deletedRecordSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request.getRequestId());
      int batchSize = Integer
        .parseInt(getProperty(request.getRequestId(), REPOSITORY_MAX_RECORDS_PER_RESPONSE));
      var oaipmhResponse = getResponseHelper().buildBaseOaipmhResponse(request);
      getNextInstances(request, batchSize, requestId, firstBatch).future()
        .onComplete(fut -> {
          if (fut.failed()) {
            logger.error("processBatch:: For requestId {} get instances failed: {}", request.getRequestId(), fut.cause()
              .getMessage());
            oaiPmhResponsePromise.fail(fut.cause());
            return;
          }

          List<JsonObject> instances = fut.result();
          logger.debug("Processing instances: {}.", instances.size());
          if (CollectionUtils.isEmpty(instances) && !firstBatch) {
            logger.error("processBatch:: For requestId {} instances collection is empty for non-first batch", request.getRequestId());
            oaiPmhResponsePromise.complete(buildBadResumptionTokenOaiResponse(oaipmhResponse, request,"Specified resumption token doesn't exists." ));
            return;
          }

          if (!firstBatch && (CollectionUtils.isNotEmpty(instances) && !instances.get(0)
            .getString(INSTANCE_ID_FIELD_NAME)
            .equals(request.getNextRecordId()))) {
            oaiPmhResponsePromise.complete(buildBadResumptionTokenOaiResponse(oaipmhResponse, request,"Stale resumption token." ));
            return;
          }

          if (CollectionUtils.isEmpty(instances)) {
            logger.debug("Got empty instances.");
            buildRecordsResponse(request, requestId, instances, lastUpdateDate, new HashMap<>(), firstBatch, null, deletedRecordSupport, statistics)
              .onSuccess(oaiPmhResponsePromise::complete)
              .onFailure(e -> handleException(oaiPmhResponsePromise, e));
            return;
          }

          String nextInstanceId = instances.size() <= batchSize ? null : instances.get(batchSize).getString(INSTANCE_ID_FIELD_NAME);
          List<JsonObject> instancesWithoutLast = nextInstanceId != null ? instances.subList(0, batchSize) : instances;
          srsClient = createAndSetupSrsClient(request);

          int retryAttempts = Integer
            .parseInt(getProperty(request.getRequestId(), REPOSITORY_SRS_HTTP_REQUEST_RETRY_ATTEMPTS));
          String targetMetadataPrefix = request.getMetadataPrefix();
          requestSRSByIdentifiers(context.owner(), instancesWithoutLast, deletedRecordSupport, retryAttempts, request)
            .onSuccess(res -> {
                if ((request.getVerb().equals(VerbType.LIST_IDENTIFIERS)
                  || MetadataPrefix.MARC21XML.getName().equals(targetMetadataPrefix)
                  || MetadataPrefix.DC.getName().equals(targetMetadataPrefix)
                ) && request.getCompleteListSize() == 0) {
                  instancesService.getTotalNumberOfRecords(request.getRequestId(), request.getTenant())
                    .onComplete(handler -> {
                      setCompleteListSize(handler, request);
                      buildRecordsResponse(request, requestId, instancesWithoutLast, lastUpdateDate, res, firstBatch, nextInstanceId,
                        deletedRecordSupport, statistics)
                        .onSuccess(oaiPmhResponsePromise::complete)
                        .onFailure(e -> oaiPmhResponsePromise.complete(buildNoRecordsFoundOaiResponse(oaipmhResponse, request, e.getMessage())));
                    });
                } else {
                  buildRecordsResponse(request, requestId, instancesWithoutLast, lastUpdateDate, res, firstBatch, nextInstanceId,
                    deletedRecordSupport, statistics)
                    .onSuccess(oaiPmhResponsePromise::complete)
                    .onFailure(e -> oaiPmhResponsePromise.complete(buildNoRecordsFoundOaiResponse(oaipmhResponse, request, e.getMessage())));
                }
              }
            )
            .onFailure(e -> oaiPmhResponsePromise.complete(buildNoRecordsFoundOaiResponse(oaipmhResponse, request, e.getMessage())));
        });
    } catch (Exception e) {
      handleException(oaiPmhResponsePromise, e);
    }
  }

  private Response buildBadResumptionTokenOaiResponse(OAIPMH oaipmh, Request request, String message) {
    oaipmh.withErrors(new OAIPMHerrorType().withCode(BAD_RESUMPTION_TOKEN).withValue(message));
    return getResponseHelper().buildFailureResponse(oaipmh, request);
  }

  private void setCompleteListSize(AsyncResult<Integer> handler, Request request) {
    if (handler.succeeded()) {
      var completeListSize = handler.result();
      request.setCompleteListSize(completeListSize);
    } else {
      logger.error("Complete list size cannot be retrieved: {}", handler.cause().getMessage(), handler.cause());
    }
  }

  private SourceStorageSourceRecordsClientWrapper createAndSetupSrsClient(Request request) {
    return new SourceStorageSourceRecordsClientWrapper(request.getOkapiUrl(), request.getTenant(), request.getOkapiToken(),
        WebClientProvider.getWebClientForSRSByTenant(request.getTenant(), request.getRequestId()));
  }

  private void downloadInstances(Request request, Promise<Response> oaiPmhResponsePromise, Promise<Object> downloadInstancesPromise,
                                 Context vertxContext, StatisticsHolder downloadInstancesStatistics) {

    HttpRequest<Buffer> httpRequest = buildInventoryQuery(request);
    PostgresClient postgresClient = PostgresClient.getInstance(vertxContext.owner(), request.getTenant());
    setupBatchHttpStream(oaiPmhResponsePromise, httpRequest, request, postgresClient, downloadInstancesPromise, downloadInstancesStatistics);
  }

    private void setupBatchHttpStream(Promise<Response> promise, HttpRequest<Buffer> inventoryHttpRequest,
                                    Request request, PostgresClient postgresClient, Promise<Object> downloadInstancesPromise, StatisticsHolder downloadInstancesStatistics) {
    var oaipmhResponse = getResponseHelper().buildBaseOaipmhResponse(request);
    String tenant = request.getTenant();
    String requestId = request.getRequestId();
    var maxChunkSize = Integer.parseInt(getProperty(requestId, REPOSITORY_FETCHING_CHUNK_SIZE));

    Promise<Boolean> responseChecked = Promise.promise();
    var jsonParser = new OaiPmhJsonParser().objectValueMode();

    var jsonWriter = new JsonWriter(jsonParser, TRACKER_LIMIT);
    var batch = new ArrayList<JsonEvent>();
    jsonParser.handler(event -> {
      batch.add(event);
      var size = batch.size();
      if (size >= maxChunkSize) {
        jsonWriter.chunkReceived();
        var chunk = new ArrayList<>(batch);
        saveInstancesIds(chunk, tenant, requestId, postgresClient).onComplete(result -> {
          if (result.succeeded()) {
            downloadInstancesStatistics.addDownloadedAndSavedInstancesCounter(size);
          } else {
            downloadInstancesStatistics.addFailedToSaveInstancesCounter(size);
            var ids = chunk.stream()
                    .map(instance -> instance.objectValue().getString(INSTANCE_ID_FIELD_NAME)).collect(toList());
            downloadInstancesStatistics.addFailedToSaveInstancesIds(ids);
          }
          jsonWriter.chunkSent();
        });
        batch.clear();
      }
    });
    jsonParser.endHandler(e -> {
      if (!batch.isEmpty()) {
        saveInstancesIds(batch, tenant, requestId, postgresClient)
          .onComplete(result -> {
            if (result.succeeded()) {
              downloadInstancesStatistics.addDownloadedAndSavedInstancesCounter(batch.size());
            } else {
              downloadInstancesStatistics.addFailedToSaveInstancesCounter(batch.size());
              var ids = batch.stream()
                      .map(instance -> instance.objectValue().getString(INSTANCE_ID_FIELD_NAME)).collect(toList());
              downloadInstancesStatistics.addFailedToSaveInstancesIds(ids);
            }
            batch.clear();
          }).onComplete(vVoid -> {
            logger.info("setupBatchHttpStream:: Completing batch processing for requestId: {}. Last batch size was: {}", requestId, batch.size());
            downloadInstancesPromise.complete();
          });
      } else {
        logger.info("setupBatchHttpStream:: Completing batch processing for requestId: {}. Last batch was empty", requestId);
        downloadInstancesPromise.complete();
      }
    });
    jsonParser.exceptionHandler(throwable -> responseChecked.future().onSuccess(invalidResponseReceivedAndProcessed -> {
        if (invalidResponseReceivedAndProcessed) {
          return;
        }
        logger.error("setupBatchHttpStream:: For requestId {} error has been occurred at JsonParser while saving instances. Message: {}", request.getRequestId(), throwable.getMessage(),
          throwable);
        downloadInstancesPromise.complete(throwable);
        promise.fail(throwable);
      })
    );

    inventoryHttpRequest.as(BodyCodec.pipe(jsonWriter, true))
      .send()
      .onSuccess(resp -> {
        switch (resp.statusCode()) {
          case 200:
            responseChecked.complete(false);
            break;
          case 403: {
            String errorMessage = getErrorFromStorageMessage(INVENTORY_STORAGE, request.getOkapiUrl() + inventoryHttpRequest.uri(), DOWNLOAD_INSTANCES_MISSED_PERMISSION);
            logger.error(errorMessage);
            promise.complete(buildNoRecordsFoundOaiResponse(oaipmhResponse, request, errorMessage));
            responseChecked.complete(true);
            break;
          } default: {
            String errorMessage = getErrorFromStorageMessage(INVENTORY_STORAGE, inventoryHttpRequest.uri(), "Invalid response: " + resp.statusMessage() + " " + resp.bodyAsString());
            logger.error(errorMessage);
            errorsService.saveErrorsAndUpdateRequestMetadata(tenant, requestId, null);
            errorMessage = format(MOD_INVENTORY_STORAGE_ERROR, request.getTenant(), resp.statusCode());
            promise.complete(buildNoRecordsFoundOaiResponse(oaipmhResponse, request, errorMessage));
            responseChecked.complete(true);
          }
        }
      })
      .onFailure(throwable -> {
        logger.error("setupBatchHttpStream:: For requestId {} error has been occurred at JsonParser while reading data from response. Message: {}", request.getRequestId(), throwable.getMessage(),
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
    paramMap.put(ONLY_INSTANCE_UPDATE_DATE, "false");

    String source = resolveRequestSource(request);
    if (StringUtils.isNotEmpty(source)) {
      paramMap.put(FOLIO_RECORD_SOURCE, source);
    }

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
    vertx.setTimer(retryCount.get() == 0 ? 50 : 2000, id -> getNextBatch(requestId, request, firstBatch, batchSize, listPromise, retryCount));
    listPromise.future()
      .compose(instances -> {
        if (CollectionUtils.isNotEmpty(instances)) {
          List<JsonObject> jsonInstances = instances.stream()
            .map(this::getInstanceAsJsonObject)
            .collect(Collectors.toList());
          if (instances.size() > batchSize) {
            request.setNextInstancePkValue(instances.get(batchSize)
              .getId());
          }
          if (request.getVerb() == VerbType.LIST_IDENTIFIERS) {
            return Future.succeededFuture(jsonInstances);
          }
          metricsCollectingService.startMetric(requestId, INSTANCES_PROCESSING);
          return enrichInstances(jsonInstances, request)
                  .onComplete(listAsyncResult -> metricsCollectingService.endMetric(requestId, INSTANCES_PROCESSING));
        }
        logger.debug("getNextInstances:: Skipping enrich instances call, empty instance ids list returned");
        return Future.succeededFuture(Collections.emptyList());
      })
      .onSuccess(promise::complete)
      .onFailure(throwable -> {
        logger.error("getNextInstances:: For requestId {} cannot get batch of instances ids from database: {}", request.getRequestId(), throwable.getMessage());
        promise.fail(throwable);
      });

    return promise;
  }

  private JsonObject getInstanceAsJsonObject(Instances instance) {
    var jsonObject = new JsonObject();
    jsonObject.put(INSTANCE_ID_FIELD_NAME, instance.getInstanceId().toString());
    jsonObject.put(SUPPRESS_FROM_DISCOVERY, instance.getSuppressFromDiscovery());
    return jsonObject;
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
              timer -> getNextBatch(requestId, request, true, batchSize, listPromise, retryCount)));

        }
        int autoIncrementedId = request.getNextInstancePkValue();
        return instancesService.getInstancesList(batchSize + 1, requestId, autoIncrementedId, request.getTenant())
          .onComplete(handleInstancesDbResponse(listPromise, streamEnded, batchSize,
            timer -> getNextBatch(requestId, request, false, batchSize, listPromise, retryCount)));

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

  private Future<Response> buildRecordsResponse(Request request, String requestId, List<JsonObject> batch, OffsetDateTime lastUpdateDate,
      Map<String, JsonObject> srsResponse, boolean firstBatch, String nextInstanceId, boolean deletedRecordSupport, StatisticsHolder statistics) {

    Promise<Response> promise = Promise.promise();
    // Set incoming instances number
    batchesSizeCounter.addAndGet(batch.size());
    try {
      List<RecordType> records = buildRecordsList(request, batch, srsResponse, deletedRecordSupport, statistics);
      logger.debug("Build records response, instances = {}, instances with srs records = {}.", batch.size(), records.size());
      ResponseHelper responseHelper = getResponseHelper();
      OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request);
      if (records.isEmpty() && nextInstanceId == null && firstBatch) {
        oaipmh.withErrors(createNoRecordsFoundError());
      } else {
        if (request.getVerb() == VerbType.LIST_IDENTIFIERS) {
          List<HeaderType> headers = records.stream().map(RecordType::getHeader).collect(toList());
          oaipmh.withListIdentifiers(new ListIdentifiersType().withHeaders(headers));
        } else {
          oaipmh.withListRecords(new ListRecordsType().withRecords(records));
        }
      }
      Response response;
      if (oaipmh.getErrors()
        .isEmpty()) {
        if (!firstBatch || nextInstanceId != null) {
          ResumptionTokenType resumptionToken = buildResumptionTokenFromRequest(request, requestId, records.size(), nextInstanceId);
          if (request.getVerb() == VerbType.LIST_IDENTIFIERS) {
            oaipmh.getListIdentifiers().withResumptionToken(resumptionToken);
          } else {
            oaipmh.getListRecords().withResumptionToken(resumptionToken);
          }
        }
        response = responseHelper.buildSuccessResponse(oaipmh);
      } else {
        response = responseHelper.buildFailureResponse(oaipmh, request);
      }
      instancesService.updateRequestUpdatedDateAndStatistics(requestId, lastUpdateDate, statistics, request.getTenant())
              .onComplete(x -> promise.complete(response));
    } catch (Exception e) {
      instancesService.updateRequestUpdatedDateAndStatistics(requestId, lastUpdateDate, statistics, request.getTenant())
              .onComplete(x -> handleException(promise, e));
    }
    return promise.future();
  }

  private List<RecordType> buildRecordsList(Request request, List<JsonObject> batch, Map<String, JsonObject> srsResponse,
      boolean deletedRecordSupport, StatisticsHolder statistics) {
    RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();

    final boolean suppressedRecordsProcessing = getBooleanProperty(request.getRequestId(),
        REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
    List<RecordType> records = new ArrayList<>();
    batch.stream()
      .filter(instance -> {
        final String instanceId = instance.getString(INSTANCE_ID_FIELD_NAME);
        final JsonObject srsInstance = srsResponse.get(instanceId);
        if (isNull(srsInstance)) {
          statistics.addSkippedInstancesCounter(1);
          statistics.addSkippedInstancesIds(instanceId);
          return false;
        }
        return true;
      })
      .forEach(instance -> {
        final String instanceId = instance.getString(INSTANCE_ID_FIELD_NAME);
        final JsonObject srsRecord = srsResponse.get(instanceId);
        RecordType instanceRecord = createRecord(request, srsRecord, instanceId);

        JsonObject updatedSrsWithItemsData = metadataManager.populateMetadataWithItemsData(srsRecord, instance,
            suppressedRecordsProcessing);
        JsonObject updatedSrsRecord = metadataManager.populateMetadataWithHoldingsData(updatedSrsWithItemsData, instance,
          suppressedRecordsProcessing);
        String source = storageHelper.getInstanceRecordSource(updatedSrsRecord);
        if (source != null && instanceRecord.getHeader()
          .getStatus() == null) {
          if (suppressedRecordsProcessing) {
            source = metadataManager.updateMetadataSourceWithDiscoverySuppressedData(source, updatedSrsRecord);
            source = metadataManager.updateElectronicAccessFieldWithDiscoverySuppressedData(source, updatedSrsRecord);
          }
          try {
            if (request.getVerb() == VerbType.LIST_RECORDS) {
              instanceRecord.withMetadata(buildOaiMetadata(request, source));
            }
          } catch (Exception e) {
            statistics.addFailedInstancesCounter(1);
            statistics.addFailedInstancesIds(instanceId);
            logger.error("Error occurred while converting record to xml representation: {}.", e.getMessage(), e);
            logger.debug("Skipping problematic record due the conversion error. Source record id - {}.",
                storageHelper.getRecordId(srsRecord));
            return;
          }
        }
        if (filterInstance(request, srsRecord)) {
          statistics.addReturnedInstancesCounter(1);
          records.add(instanceRecord);
        } else {
          statistics.addSuppressedInstancesCounter(1);
          statistics.addSuppressedInstancesIds(instanceId);
        }
      });
    return records;
  }

  private ResumptionTokenType buildResumptionTokenFromRequest(Request request, String requestId, long returnedCount,
      String nextInstanceId) {
    long cursor = request.getOffset();
    if (nextInstanceId == null) {
      logHarvestingCompletion();
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

    var resumptionTokenType = new ResumptionTokenType()
      .withExpirationDate(Instant.now().with(ChronoField.NANO_OF_SECOND, 0).plusSeconds(RESUMPTION_TOKEN_TIMEOUT))
      .withCursor(BigInteger.valueOf(cursor));
    String targetMetadataPrefix = request.getMetadataPrefix();
    if ((request.getVerb().equals(VerbType.LIST_IDENTIFIERS)
      || MetadataPrefix.MARC21XML.getName().equals(targetMetadataPrefix)
      || MetadataPrefix.DC.getName().equals(targetMetadataPrefix)) && request.getCompleteListSize() > 0) {
      resumptionTokenType.withCompleteListSize(BigInteger.valueOf(request.getCompleteListSize()));
      extraParams.put(REQUEST_COMPLETE_LIST_SIZE_PARAM, String.valueOf(request.getCompleteListSize()));
    }
    String resumptionToken = request.toResumptionToken(extraParams);
    resumptionTokenType.withValue(resumptionToken);
    return resumptionTokenType;
  }

  private void logHarvestingCompletion() {
    logger.info("Harvesting completed. Number of processed instances: {}.", batchesSizeCounter.get());
    batchesSizeCounter.setRelease(0);
  }

  private Future<Void> saveInstancesIds(List<JsonEvent> instances, String tenant, String requestId,
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
    return promise.future();
  }

  private Future<Void> saveInstances(List<Instances> instances, String tenantId, String requestId, PostgresClient postgresClient) {
    if (instances.isEmpty()) {
      logger.debug("Skip saving instances. Instances list is empty.");
      return Future.succeededFuture();
    }

    Promise<Void> promise = Promise.promise();
    postgresClient.getConnection(e -> {
      List<Tuple> batch = new ArrayList<>();
      instances.forEach(inst -> batch.add(Tuple.of(inst.getInstanceId(), UUID.fromString(requestId), inst.getSuppressFromDiscovery(),
        inst.getSource())));
      String sql = "INSERT INTO " + PostgresClient.convertToPsqlStandard(tenantId)
          + ".instances (instance_id, request_id, suppress_from_discovery, source) VALUES ($1, $2, $3, $4) RETURNING instance_id";

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
        .setSuppressFromDiscovery(Boolean.parseBoolean(inst.getString(SUPPRESS_FROM_DISCOVERY)))
        .setRequestId(requestId).setSource(inst.getString(FOLIO_RECORD_SOURCE)))
      .collect(Collectors.toList());
  }

  private Future<Map<String, JsonObject>> requestSRSByIdentifiers(Vertx vertx, List<JsonObject> batch, boolean deletedRecordSupport,
                                                                  int retryAttempts, Request request) {
    final List<String> listOfIds = extractListOfIdsForSRSRequest(batch);
    logger.debug("Request to SRS, list id size: {}.", listOfIds.size());
    AtomicInteger attemptsCount = new AtomicInteger(retryAttempts);
    Promise<Map<String, JsonObject>> promise = Promise.promise();
    doPostRequestToSrs(vertx, deletedRecordSupport, listOfIds, attemptsCount, retryAttempts, promise,
      request);
    return promise.future();
  }

  private void doPostRequestToSrs(Vertx vertx, boolean deletedRecordSupport,
                                  List<String> listOfIds, AtomicInteger attemptsCount, int retryAttempts, Promise<Map<String, JsonObject>> promise,
                                  Request request) {
    var recordsSource = getProperty(request.getRequestId(), REPOSITORY_RECORDS_SOURCE);
    if (!recordsSource.equals(INVENTORY)) {
      try {
        srsClient.postSourceStorageSourceRecords("INSTANCE", null, deletedRecordSupport, listOfIds, asyncResult -> {
          Map<String, String> retrySRSRequestParams = new HashMap<>();
          if (asyncResult.succeeded()) {
            HttpResponse<Buffer> srsResponse = asyncResult.result();
            int statusCode = srsResponse.statusCode();
            String statusMessage = srsResponse.statusMessage();
            if (statusCode >= 400) {
              retrySRSRequestParams.put(RETRY_ATTEMPTS, String.valueOf(retryAttempts));
              retrySRSRequestParams.put(STATUS_CODE, String.valueOf(statusCode));
              retrySRSRequestParams.put(STATUS_MESSAGE, statusMessage);
              retrySRSRequest(vertx, deletedRecordSupport, listOfIds, attemptsCount, promise, retrySRSRequestParams,
                request);
              return;
            }
            if (statusCode != 200) {
              String errorMsg = getErrorFromStorageMessage("source-record-storage", "/source-storage/source-records",
                srsResponse.statusMessage());
              handleException(promise, new IllegalStateException(errorMsg));
              return;
            }
            handleSrsResponse(promise, srsResponse.body(), request, listOfIds);
          } else {
            logger.error("Error has been occurred while requesting the SRS: {}.", asyncResult.cause()
              .getMessage(), asyncResult.cause());
            retrySRSRequestParams.put(RETRY_ATTEMPTS, String.valueOf(retryAttempts));
            retrySRSRequestParams.put(STATUS_CODE, String.valueOf(-1));
            retrySRSRequestParams.put(STATUS_MESSAGE, "");
            retrySRSRequest(vertx, deletedRecordSupport, listOfIds, attemptsCount, promise, retrySRSRequestParams,
              request);
          }
        });
      } catch (Exception e) {
        handleException(promise, e);
      }
    } else {
      doGetRequestToInventory(request, promise, Maps.newHashMap(), new JsonArray(), listOfIds);
    }
  }

  private void doGetRequestToInventory(Request request, Promise<Map<String, JsonObject>> promise, Map<String, JsonObject> result,
                                       JsonArray records, List<String> listOfIds) {
    int limit = Integer.parseInt(getProperty(request.getRequestId(), REPOSITORY_MAX_RECORDS_PER_RESPONSE));
    @SuppressWarnings("rawtypes")
    List<Future> allParts = new ArrayList<>();
    ListUtils.partition(listOfIds, MAX_RECORDS_PER_REQUEST_FROM_INVENTORY).forEach(part -> {
      var future = requestFromInventory(request, limit, getInstanceIdForInventorySearch(request, part), true, true).onComplete(instancesHandler -> {
        if (instancesHandler.succeeded()) {
          var inventoryRecords = instancesHandler.result();
          if (request.getVerb() != VerbType.LIST_IDENTIFIERS) {
            generateRecordsOnTheFly(request, inventoryRecords);
          }
          inventoryRecords.getJsonArray("instances").forEach(instance -> {
            var jsonInstance = (JsonObject) instance;
            var externalIdsHolder = new JsonObject();
            externalIdsHolder.put(INSTANCE_ID_FIELD_NAME, jsonInstance.getString("id"));
            jsonInstance.put("externalIdsHolder", externalIdsHolder);
            records.add(jsonInstance);
          });
        } else {
          handleException(promise, instancesHandler.cause());
          promise.fail(instancesHandler.cause());
        }
      });
      allParts.add(future);
    });
    CompositeFuture.join(allParts).onComplete(handler -> {
      if (handler.succeeded()) {
        buildResult(records, result, promise);
      }
    });
  }

  private List<String> getInstanceIdForInventorySearch(Request request, List<String> listOfIds) {
    if (nonNull(listOfIds)) {
      return listOfIds;
    }
    return request.getIdentifier() != null ? List.of(request.getStorageIdentifier()) : null;
  }

  private void retrySRSRequest(Vertx vertx, boolean deletedRecordSupport,
      List<String> listOfIds, AtomicInteger attemptsCount, Promise<Map<String, JsonObject>> promise, Map <String, String> retrySRSRequestParams,
                               Request request) {
    if (Integer.parseInt(retrySRSRequestParams.get(STATUS_CODE)) > 0) {
      logger.debug("Got error response form SRS, status code: {}, status message: {}.",
        Integer.parseInt(retrySRSRequestParams.get(STATUS_CODE)), retrySRSRequestParams.get(STATUS_MESSAGE));
    } else {
      logger.debug("Error has been occurred while requesting SRS.");
    }
    if (attemptsCount.decrementAndGet() <= 0) {
      String errorMessage = "mod-source-record-storage didn't respond with expected status code after " + Integer.parseInt(retrySRSRequestParams.get(RETRY_ATTEMPTS))
          + " attempts. Canceling further request processing.";
      handleException(promise, new IllegalStateException(errorMessage));
      return;
    }
    logger.debug("Trying to request SRS again, attempts left: {}.", attemptsCount.get());
    vertx.setTimer(REREQUEST_SRS_DELAY,
        timer -> doPostRequestToSrs(vertx, deletedRecordSupport, listOfIds, attemptsCount,
          Integer.parseInt(retrySRSRequestParams.get(RETRY_ATTEMPTS)), promise, request));
  }

  private void handleSrsResponse(Promise<Map<String, JsonObject>> promise, Buffer buffer, Request request, List<String> listOfIds) {
    final Map<String, JsonObject> result = Maps.newHashMap();
    try {
      final Object jsonResponse = buffer.toJson();
      if (jsonResponse instanceof JsonObject) {
        JsonObject entries = (JsonObject) jsonResponse;
        final JsonArray records = entries.getJsonArray("sourceRecords");
        var recordsSource = getProperty(request.getRequestId(), REPOSITORY_RECORDS_SOURCE);
        if (!recordsSource.equals(SRS)) {
          doGetRequestToInventory(request, promise, result, records, listOfIds);
        } else {
          buildResult(records, result, promise);
        }
      } else {
        logger.debug("Can't process SRS response: {}.", buffer);
      }
    } catch (DecodeException ex) {
      String msg = "Invalid json has been returned from SRS, cannot parse response to json.";
      handleException(promise, new IllegalStateException(msg, ex));
    } catch (Exception e) {
      handleException(promise, e);
    }
  }

  private void buildResult(JsonArray records, Map<String, JsonObject> result, Promise<Map<String, JsonObject>> promise) {
    records.stream()
      .filter(Objects::nonNull)
      .map(JsonObject.class::cast)
      .forEach(jo -> result.put(jo.getJsonObject("externalIdsHolder")
        .getString(INSTANCE_ID_FIELD_NAME), jo));
    promise.complete(result);
  }

  private List<String> extractListOfIdsForSRSRequest(List<JsonObject> batch) {

    return batch.stream()
      .filter(Objects::nonNull)
      .map(instance -> instance.getString(INSTANCE_ID_FIELD_NAME))
      .collect(toList());
  }

  @Override
  protected List<OAIPMHerrorType> validateRequest(Request request) {
    return validateListRequest(request);
  }

  @Autowired
  public void setInstancesService(InstancesService instancesService) {
    this.instancesService = instancesService;
  }

}
