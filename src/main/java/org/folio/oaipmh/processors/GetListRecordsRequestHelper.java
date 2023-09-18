package org.folio.oaipmh.processors;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.domain.StatisticsHolder;
import org.folio.oaipmh.helpers.AbstractGetRecordsHelper;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.querybuilder.QueryBuilder;
import org.folio.oaipmh.querybuilder.QueryException;
import org.folio.oaipmh.querybuilder.RecordsSource;
import org.folio.oaipmh.service.ConsortiaService;
import org.folio.oaipmh.service.InstancesService;
import org.folio.oaipmh.service.MetricsCollectingService;
import org.folio.oaipmh.service.SourceStorageSourceRecordsClientWrapper;
import org.folio.oaipmh.service.ViewsService;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
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

import javax.ws.rs.core.Response;
import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.folio.oaipmh.Constants.EXPIRATION_DATE_RESUMPTION_TOKEN_PARAM;
import static org.folio.oaipmh.Constants.FROM_DELETED_PARAM;
import static org.folio.oaipmh.Constants.INSTANCE_ID_FROM_VIEW_RESPONSE;
import static org.folio.oaipmh.Constants.LAST_INSTANCE_ID_PARAM;
import static org.folio.oaipmh.Constants.MARC_RECORD_FROM_VIEW_RESPONSE;
import static org.folio.oaipmh.Constants.NEXT_INSTANCE_PK_VALUE;
import static org.folio.oaipmh.Constants.NEXT_RECORD_ID_PARAM;
import static org.folio.oaipmh.Constants.OFFSET_PARAM;
import static org.folio.oaipmh.Constants.PARSED_RECORD;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_RECORDS_SOURCE;
import static org.folio.oaipmh.Constants.REPOSITORY_SRS_HTTP_REQUEST_RETRY_ATTEMPTS;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.REQUEST_COMPLETE_LIST_SIZE_PARAM;
import static org.folio.oaipmh.Constants.REQUEST_ID_PARAM;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_TIMEOUT;
import static org.folio.oaipmh.Constants.RETRY_ATTEMPTS;
import static org.folio.oaipmh.Constants.SOURCE;
import static org.folio.oaipmh.Constants.STATUS_CODE;
import static org.folio.oaipmh.Constants.STATUS_MESSAGE;
import static org.folio.oaipmh.Constants.TOTAL_RECORDS_PARAM;
import static org.folio.oaipmh.Constants.TURNED_TO_DELETED_PARAM;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getProperty;
import static org.folio.oaipmh.querybuilder.RecordsSource.CONSORTIUM_MARC;
import static org.folio.oaipmh.querybuilder.RecordsSource.MARC_SHARED;
import static org.folio.oaipmh.service.MetricsCollectingService.MetricOperation.INSTANCES_PROCESSING;
import static org.folio.oaipmh.service.MetricsCollectingService.MetricOperation.SEND_REQUEST;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

public class GetListRecordsRequestHelper extends AbstractGetRecordsHelper {

  protected final Logger logger = LogManager.getLogger(getClass());

  private static final int REREQUEST_SRS_DELAY = 2000;
  private static final long MAX_EVENT_LOOP_EXECUTE_TIME_NS = 60_000_000_000L;

  public static final GetListRecordsRequestHelper INSTANCE = new GetListRecordsRequestHelper();

  private final Vertx vertx;
  private final AtomicInteger batchesSizeCounter = new AtomicInteger();

  private final MetricsCollectingService metricsCollectingService = MetricsCollectingService.getInstance();
  private InstancesService instancesService;

  private ConsortiaService consortiaService;

  private ViewsService viewsService;

  public static GetListRecordsRequestHelper getInstance() {
    return INSTANCE;
  }

  private GetListRecordsRequestHelper() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    var vertxOptions = new VertxOptions();
    vertxOptions.setMaxEventLoopExecuteTime(MAX_EVENT_LOOP_EXECUTE_TIME_NS);
    vertx = Vertx.vertx(vertxOptions);
  }

  /**
   * Handle MarcWithHoldings request
   */
  @Override
  public Future<Response> handle(Request request, Context vertxContext) {
    long t = System.nanoTime();
    Promise<Response> oaipmhResponsePromise = Promise.promise();
    metricsCollectingService.startMetric(request.getRequestId(), SEND_REQUEST);
    try {
      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, oaipmhResponsePromise, errors);
      }

      OffsetDateTime lastUpdateDate = OffsetDateTime.now(ZoneId.systemDefault());

      handleRequestMetadata(request, lastUpdateDate);

      var batchInstancesStatistics = new StatisticsHolder();

      boolean isFirstBatch = request.getResumptionToken() == null;

      processBatch(request, oaipmhResponsePromise, isFirstBatch, batchInstancesStatistics, lastUpdateDate);

    } catch (Exception e) {
      handleException(oaipmhResponsePromise, e);
    }
    return oaipmhResponsePromise.future().onComplete(responseAsyncResult -> {
      logger.info("Total time for response: {} sec", (System.nanoTime() - t) / 1_000_000_000);
      metricsCollectingService.endMetric(request.getRequestId(), SEND_REQUEST);
    });
  }

  private void handleRequestMetadata(Request request, OffsetDateTime lastUpdateDate) {
    RequestMetadataLb requestMetadata = new RequestMetadataLb().setLastUpdatedDate(lastUpdateDate);
    String resumptionToken = request.getResumptionToken();
    if (resumptionToken == null) {
      requestMetadata.setRequestId(UUID.fromString(request.getRequestId()));
      if (request.getCursor() == 0) {
        requestMetadata.setStartedDate(lastUpdateDate);
      }
      instancesService.saveRequestMetadata(requestMetadata, request.getTenant());
    }
  }

  private void processBatch(Request request, Promise<Response> oaiPmhResponsePromise,
                            boolean firstBatch, StatisticsHolder statistics, OffsetDateTime lastUpdateDate) {
    try {
      var oaipmhResponse = getResponseHelper().buildBaseOaipmhResponse(request);
      String targetMetadataPrefix = request.getMetadataPrefix();
      int retryAttempts = Integer
        .parseInt(getProperty(request.getRequestId(), REPOSITORY_SRS_HTTP_REQUEST_RETRY_ATTEMPTS));
      boolean supportCompletedSize =
        (request.getVerb().equals(VerbType.LIST_IDENTIFIERS)
          || MetadataPrefix.MARC21XML.getName().equals(targetMetadataPrefix)
          || MetadataPrefix.DC.getName().equals(targetMetadataPrefix)
        ) && request.getCompleteListSize() == 0;
      requestRecords(request, retryAttempts, supportCompletedSize, statistics)
        .onSuccess(records -> {
          if (records.isEmpty() && !firstBatch) {
            logger.error("processBatch:: For requestId {} instances collection is empty for non-first batch", request.getRequestId());
            oaiPmhResponsePromise.complete(buildBadResumptionTokenOaiResponse(oaipmhResponse, request,"Specified resumption token doesn't exists." ));
            return;
          }
            var listRecords = records.stream().map(JsonObject.class::cast).collect(toList());
            long t = System.nanoTime();
            enrichInstances(listRecords, request)
              .onComplete(listAsyncResult -> {
                logger.info("After enrich: {} sec", (System.nanoTime() - t) / 1_000_000_000);
                if (listAsyncResult.succeeded()) {
                  buildRecordsResponse(request, lastUpdateDate, new JsonArray(listAsyncResult.result()), firstBatch, statistics)
                    .onSuccess(oaiPmhResponsePromise::complete)
                    .onFailure(e -> oaiPmhResponsePromise.complete(buildNoRecordsFoundOaiResponse(oaipmhResponse, request, e.getMessage())));
                  metricsCollectingService.endMetric(request.getRequestId(), INSTANCES_PROCESSING);
                } else {
                  logger.error("Request records failed : {}", listAsyncResult.cause().getMessage(), listAsyncResult.cause());
                }
              });
          }
        )
        .onFailure(e -> oaiPmhResponsePromise.complete(buildNoRecordsFoundOaiResponse(oaipmhResponse, request, e.getMessage())));
    } catch (Exception e) {
      handleException(oaiPmhResponsePromise, e);
    }
  }

  private Response buildBadResumptionTokenOaiResponse(OAIPMH oaipmh, Request request, String message) {
    oaipmh.withErrors(new OAIPMHerrorType().withCode(BAD_RESUMPTION_TOKEN).withValue(message));
    return getResponseHelper().buildFailureResponse(oaipmh, request);
  }

  private Future<JsonArray> requestRecords(Request request, int retryAttempts, boolean supportCompletedSize,
                                           StatisticsHolder statistics) {
    var recordsSource = getProperty(request.getRequestId(), REPOSITORY_RECORDS_SOURCE);
    int limit = Integer.parseInt(getProperty(request.getRequestId(), REPOSITORY_MAX_RECORDS_PER_RESPONSE)) + 1;
    boolean skipSuppressedFromDiscovery = isSkipSuppressed(request);
    boolean deletedRecordsSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request.getRequestId());
    var dateFrom = convertStringToDate(request.getFrom(), false, false);
    var dateUntil = convertStringToDate(request.getUntil(), true, false);
    String from = null;
    String until = null;
    if (nonNull(dateFrom)) {
      from = dateFormat.format(dateFrom);
    }
    if (nonNull(dateUntil)) {
      until = dateFormat.format(dateUntil);
    }
    AtomicInteger attemptsCount = new AtomicInteger(retryAttempts);

    JsonArray finalRecords = new JsonArray();

    Promise<JsonArray> local = Promise.promise();
    Promise<JsonArray> shared = Promise.promise();

    logger.info("Before doRequest, limit: {}", limit);
    long t = System.nanoTime();
    doRequest(request, finalRecords, skipSuppressedFromDiscovery, deletedRecordsSupport, from, until,
      RecordsSource.getSource(recordsSource), limit, limit, supportCompletedSize, statistics)
      .onComplete(handler -> {
        if (handler.succeeded()) {
          logger.info("After doRequest, total time to complete: {} sec, found records: {}",
            (System.nanoTime() - t) / 1_000_000_000, finalRecords.size());
          var idJsonMap = finalRecords.stream().map(JsonObject.class::cast)
            .filter(json -> json.getString(SOURCE).equals(MARC_SHARED.toString()) ||
              json.getString(SOURCE).equals(CONSORTIUM_MARC.toString()))
              .collect(toMap(jsonKey -> jsonKey.getString(INSTANCE_ID_FROM_VIEW_RESPONSE), jsonValue -> jsonValue,
                (id1, id2) -> id1)); // Here id1 = id2, i.e. the same ids, take only first one.
          if (!idJsonMap.isEmpty()) {
            excludeSharedMarcRecords(finalRecords, idJsonMap);
            var centralTenantId = consortiaService.getCentralTenantId(request);
            if (StringUtils.isNotEmpty(centralTenantId)) {
              doRequestShared(vertx, deletedRecordsSupport, idJsonMap, attemptsCount, retryAttempts,
                shared, new Request(request, centralTenantId), finalRecords);
            } else {
              shared.complete(new JsonArray());
            }
          } else {
            shared.complete(new JsonArray());
          }
          updateWithParsedRecord(finalRecords, local, request);
        } else {
          logger.error("Request records was not succeeded: {}", handler.cause().getMessage(), handler.cause());
        }
      });

    return CompositeFuture.join(shared.future(), local.future())
      .map(CompositeFuture::list)
      .map(results -> results.stream()
        .map(JsonArray.class::cast)
        .reduce(new JsonArray(), JsonArray::addAll)
      );
  }

  private void excludeSharedMarcRecords(JsonArray finalRecords, Map<String, JsonObject> idJsonMap) {
    var iterator = finalRecords.iterator();
    while (iterator.hasNext()) {
      var rec = (JsonObject)iterator.next();
      if (idJsonMap.keySet().contains(rec.getString(INSTANCE_ID_FROM_VIEW_RESPONSE))) {
        iterator.remove();
      }
    }
  }

  private void doRequestShared(Vertx vertx, boolean deletedRecordsSupport, Map<String, JsonObject> idJsonMap,
                               AtomicInteger attemptsCount, int retryAttempts, Promise<JsonArray> promise,
                               Request request, JsonArray records) {
    logger.info("doRequestShared execution");
    SourceStorageSourceRecordsClientWrapper.getSourceStorageSourceRecordsClient(request).postSourceStorageSourceRecords("INSTANCE",
      null, deletedRecordsSupport, new ArrayList<>(idJsonMap.keySet()), asyncResult -> {
      Map<String, String> retrySRSRequestParams = new HashMap<>();
      if (asyncResult.succeeded()) {
        HttpResponse<Buffer> srsResponse = asyncResult.result();
        int statusCode = srsResponse.statusCode();
        String statusMessage = srsResponse.statusMessage();
        if (statusCode >= 400) {
          retrySRSRequestParams.put(RETRY_ATTEMPTS, String.valueOf(retryAttempts));
          retrySRSRequestParams.put(STATUS_CODE, String.valueOf(statusCode));
          retrySRSRequestParams.put(STATUS_MESSAGE, statusMessage);
          retrySRSRequest(vertx, deletedRecordsSupport, idJsonMap, attemptsCount, promise, retrySRSRequestParams,
            request, records);
        }
        if (statusCode != 200) {
          String errorMsg = getErrorFromStorageMessage("source-record-storage", "/source-storage/source-records",
            srsResponse.statusMessage());
          handleException(promise, new IllegalStateException(errorMsg));
          logger.error("Error response: {}", srsResponse.bodyAsString());
        }
        JsonArray sourceRecords = srsResponse.bodyAsJsonObject().getJsonArray("sourceRecords");
        sourceRecords.stream().map(JsonObject.class::cast)
          .forEach(json -> {
            var instId = json.getJsonObject("externalIdsHolder").getString("instanceId");
            var parsedRecord = json.getJsonObject(PARSED_RECORD);
            var existingRecord = idJsonMap.get(instId);
            existingRecord.put(PARSED_RECORD, parsedRecord);
          });
        promise.complete(new JsonArray(new ArrayList<>(idJsonMap.values())));
      } else {
        logger.error("Error has been occurred while requesting the SRS: {}.", asyncResult.cause()
          .getMessage(), asyncResult.cause());
        retrySRSRequestParams.put(RETRY_ATTEMPTS, String.valueOf(retryAttempts));
        retrySRSRequestParams.put(STATUS_CODE, String.valueOf(-1));
        retrySRSRequestParams.put(STATUS_MESSAGE, "");
        retrySRSRequest(vertx, deletedRecordsSupport, idJsonMap, attemptsCount, promise, retrySRSRequestParams,
          request, records);
      }
    });
  }

  private void retrySRSRequest(Vertx vertx, boolean deletedRecordsSupport,
                               Map<String, JsonObject> idJsonMap, AtomicInteger attemptsCount, Promise<JsonArray> promise, Map <String, String> retrySRSRequestParams,
                               Request request, JsonArray records) {
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
      timer -> doRequestShared(vertx, deletedRecordsSupport, idJsonMap, attemptsCount,
        Integer.parseInt(retrySRSRequestParams.get(RETRY_ATTEMPTS)), promise, request, records));
  }

  private Future<Void> doRequest(Request request, JsonArray finalRecords,
                                 boolean skipSuppressedFromDiscovery, boolean deletedRecordsSupport, String from,
                                 String until, RecordsSource source, int limit, int currentLimit, boolean supportCompletedSize,
                                 StatisticsHolder statistics) {
    try {
      var query = QueryBuilder.build(request.getTenant(), request.getLastInstanceId(),
        from, until, source, skipSuppressedFromDiscovery, request.isFromDeleted() && deletedRecordsSupport,
        currentLimit);
      logger.info("Query: {}", query);
      return viewsService.query(query, request.getTenant())
        .compose(currentRecords -> handleCompleteListSize(request, supportCompletedSize, currentRecords))
        .onFailure(completeListSizeHandlerExc -> logger.error("Error occurred while calling a view: {}.",
          completeListSizeHandlerExc.getMessage(), completeListSizeHandlerExc))
        .compose(currentRecords -> processInstancesFromDB(request, currentRecords, finalRecords,
          skipSuppressedFromDiscovery, deletedRecordsSupport, from, until, source, supportCompletedSize, statistics, limit))
        .onFailure(collectDeletedHandlerExc -> logger.error("Error occurred while collecting deleted: {}.",
          collectDeletedHandlerExc.getMessage(), collectDeletedHandlerExc));
    } catch (QueryException exc) {
      logger.error("Error occurred while building a query: {}.", exc.getMessage(), exc);
      return Future.failedFuture(exc);
    }
  }

  private Future<Void> processInstancesFromDB(Request request, JsonArray currentRecords, JsonArray finalRecords,
                                              boolean skipSuppressedFromDiscovery, boolean deletedRecordsSupport,
                                              String from, String until, RecordsSource source,
                                              boolean supportCompletedSize, StatisticsHolder statistics, int limit) {
    finalRecords.addAll(currentRecords);

    logger.info("Number of records before handleDeletedRecords found: {}", currentRecords.size());

    return handleDeletedRecords(request, finalRecords, skipSuppressedFromDiscovery,
      deletedRecordsSupport, from, until, source, supportCompletedSize, statistics, limit);
  }

  private Future<Void> handleDeletedRecords(Request request, JsonArray finalRecords,
                                            boolean skipSuppressedFromDiscovery, boolean deletedRecordsSupport,
                                            String from, String until, RecordsSource source,
                                            boolean supportCompletedSize, StatisticsHolder statistics,
                                            int limit) {
    // Collect deleted if non-deleted exhausted.
    if (deletedRecordsSupport && !request.isFromDeleted()) {
      int remainingFromDeleted = limit - finalRecords.size();
      logger.info("Deleted required: {}", remainingFromDeleted);

      if (remainingFromDeleted > 0 || finalRecords.isEmpty()) {
        request.setLastInstanceId(null); // Last instance id should be null when switching to deleted.
        request.setFromDeleted(true);
        if (remainingFromDeleted == 1) {
          request.setTurnedToDeleted(true);
        }
        return doRequest(request, finalRecords, skipSuppressedFromDiscovery, true,
          from, until, source, limit, remainingFromDeleted, supportCompletedSize, statistics);
      }
    }
    updateRequest(request, finalRecords, limit, statistics);
    return Future.succeededFuture();
  }

  private void updateRequest(Request request, JsonArray finalRecords, int limit, StatisticsHolder statistics) {
    if (finalRecords.size() > 1 && finalRecords.size() >= limit) {
      request.setNextRecordId(
        finalRecords.getJsonObject(finalRecords.size() - 1).getString(INSTANCE_ID_FROM_VIEW_RESPONSE));
      if (!request.isTurnedToDeleted()) {
        request.setLastInstanceId(
          finalRecords.getJsonObject(finalRecords.size() - 2).getString(INSTANCE_ID_FROM_VIEW_RESPONSE));
      }
    } else {
      request.setNextRecordId(null); // End of harvest.
    }
    if (finalRecords.size() >= limit) {
      int diff = finalRecords.size() - limit;
      finalRecords.getList().subList(finalRecords.size() - 1, finalRecords.size() + diff).clear();
    }

    Long t = System.nanoTime();
    var filteredEmpty = handleMARCsWithoutSRSRecord(request, finalRecords, statistics);
    logger.debug("Execution of handleMARCsWithoutSRSRecord: {} sec", (System.nanoTime() - t) / 1_000_000_000);

    finalRecords.clear();
    finalRecords.addAll(filteredEmpty);
    if (request.isTurnedToDeleted()) {
      request.setTurnedToDeleted(false);
    }
  }

  private JsonArray handleMARCsWithoutSRSRecord(Request request, JsonArray currentRecords, StatisticsHolder statistics) {
    return new JsonArray(currentRecords.stream().map(JsonObject.class::cast)
      .filter(rec -> {

        // Keep FOLIO and shared as well as all deleted records.
        if (!rec.getString(SOURCE).equals(RecordsSource.MARC.name()) || Boolean.TRUE.equals(rec.getBoolean("deleted"))) {
          return true;
        }

        var marcRecord = rec.getString(MARC_RECORD_FROM_VIEW_RESPONSE);

        if (isNull(marcRecord)) { // If MARC does not have underlying SRS record.
          var instanceId = rec.getString(INSTANCE_ID_FROM_VIEW_RESPONSE);
          statistics.addSkippedInstancesCounter(1);
          statistics.addSkippedInstancesIds(instanceId);
          logger.error("MARC with null marc_record: {}", instanceId);
          errorsService.log(request.getTenant(), request.getRequestId(), rec.getString(INSTANCE_ID_FROM_VIEW_RESPONSE),
            "There is no corresponding SRS record for this MARC");
        }

        return nonNull(marcRecord);
      }).collect(toList()));
  }

  private Future<JsonArray> handleCompleteListSize(Request request, boolean supportCompletedSize, JsonArray currentRecords) {
    if (supportCompletedSize && request.getCompleteListSize() == 0) {
      return requestFromInventory(request, 1, null, false, false, false).compose(counts -> {
          var completeListSize = counts.getInteger(TOTAL_RECORDS_PARAM);
          request.setCompleteListSize(completeListSize);
          return Future.succeededFuture(currentRecords);
        })
        .onFailure(handleException -> logger.error("Error occurred while calling /inventory-storage/instances to get complete list size: {}.",
          handleException.getMessage(), handleException));
    }
    return Future.succeededFuture(currentRecords);
  }

  private Future<Response> buildRecordsResponse(Request request, OffsetDateTime lastUpdateDate,
      JsonArray finalRecords, boolean firstBatch, StatisticsHolder statistics) {
    logger.info("finalRecords size: {}", finalRecords.size());
    long t = System.nanoTime();
    var nextInstanceId = request.getNextRecordId();
    Promise<Response> promise = Promise.promise();
    try {
      List<RecordType> records = buildRecordsList(request, finalRecords, statistics);
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
          ResumptionTokenType resumptionToken = buildResumptionTokenFromRequest(request, request.getRequestId(), records.size(), nextInstanceId);
          if (request.getVerb() == VerbType.LIST_IDENTIFIERS) {
            oaipmh.getListIdentifiers().withResumptionToken(resumptionToken);
          } else {
            oaipmh.getListRecords().withResumptionToken(resumptionToken);
          }
        } else {
          saveErrorsIfExist(request); // End of single harvest (no resumption token).
        }
        response = responseHelper.buildSuccessResponse(oaipmh);
      } else {
        response = responseHelper.buildFailureResponse(oaipmh, request);
      }
      instancesService.updateRequestUpdatedDateAndStatistics(request.getRequestId(), lastUpdateDate, statistics, request.getTenant())
              .onComplete(x -> {
                logger.debug("Execution of buildRecordsResponse: {} sec", (System.nanoTime() - t) / 1_000_000_000);
                promise.complete(response);
              });
    } catch (Exception e) {
      instancesService.updateRequestUpdatedDateAndStatistics(request.getRequestId(), lastUpdateDate, statistics, request.getTenant())
              .onComplete(x -> handleException(promise, e));
    }
    return promise.future();
  }

  private List<RecordType> buildRecordsList(Request request, JsonArray srsResponse, StatisticsHolder statistics) {
    RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
    long t = System.nanoTime();
    final boolean suppressedRecordsProcessing = getBooleanProperty(request.getRequestId(),
        REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
    List<RecordType> records = new ArrayList<>();
    srsResponse.stream()
      .map(JsonObject.class::cast)
      .forEach(instance -> {
        final String instanceId = ofNullable(instance.getString("id"))
          .orElse(instance.getString(INSTANCE_ID_FROM_VIEW_RESPONSE));
        RecordType instanceRecord = createRecord(request, instance, instanceId);

        JsonObject updatedSrsWithItemsData = metadataManager.populateMetadataWithItemsData(instance, instance,
            suppressedRecordsProcessing);
        JsonObject updatedSrsRecord = metadataManager.populateMetadataWithHoldingsData(updatedSrsWithItemsData, instance,
          suppressedRecordsProcessing);
        String source = storageHelper.getInstanceRecordSource(updatedSrsRecord);
        if (source != null && instanceRecord.getHeader().getStatus() == null) {
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
                storageHelper.getRecordId(instance));
            errorsService.log(request.getTenant(), request.getRequestId(), instanceId,
              "Error occurred while converting record to xml representation: " + e.getMessage());
            return;
          }
        }
        if (filterInstance(request, instance)) {
          statistics.addReturnedInstancesCounter(1);
          records.add(instanceRecord);
        } else {
          statistics.addSuppressedInstancesCounter(1);
          statistics.addSuppressedInstancesIds(instanceId);
        }
      });
    logger.info("Execution of buildRecordsList: {} sec", (System.nanoTime() - t) / 1_000_000_000);
    return records;
  }

  private ResumptionTokenType buildResumptionTokenFromRequest(Request request, String requestId, long returnedCount,
      String nextInstanceId) {
    long t = System.nanoTime();
    long cursor = request.getOffset();
    if (nextInstanceId == null) {
      logHarvestingCompletion();
      saveErrorsIfExist(request); // End of sequential harvest.
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
    String lastInstanceId = request.getLastInstanceId();
    if (nonNull(lastInstanceId)) {
      extraParams.put(LAST_INSTANCE_ID_PARAM, lastInstanceId);
    }

    boolean fromDeleted = request.isFromDeleted();
    extraParams.put(FROM_DELETED_PARAM, String.valueOf(fromDeleted));
    boolean turnedToDeleted = request.isTurnedToDeleted();
    extraParams.put(TURNED_TO_DELETED_PARAM, String.valueOf(turnedToDeleted));

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
    logger.debug("Execution of buildResumptionTokenFromRequest: {} sec", (System.nanoTime() - t) / 1_000_000_000);
    return resumptionTokenType;
  }

  private void logHarvestingCompletion() {
    logger.info("Harvesting completed. Number of processed instances: {}.", batchesSizeCounter.get());
    batchesSizeCounter.setRelease(0);
  }

  private void updateWithParsedRecord(JsonArray records, Promise<JsonArray> promise, Request request) {
    JsonArray updatedWithParsedRecord = new JsonArray();
    long t = System.nanoTime();
    records.stream()
      .filter(Objects::nonNull)
      .map(JsonObject.class::cast)
      .forEach(jo -> {
        var marcRecord = jo.getJsonObject(MARC_RECORD_FROM_VIEW_RESPONSE);
        if (isNull(marcRecord)) {
          if (request.getVerb() != VerbType.LIST_IDENTIFIERS) {
            // Generate on the fly if no marc.
            JsonObject jsonInstances = new JsonObject();
            JsonArray jsonRecords = new JsonArray();
            jsonRecords.add(jo.getJsonObject("instance_record"));
            jsonInstances.put("instances", jsonRecords);

            generateRecordsOnTheFly(request, jsonInstances);

            var instanceRecordFromViewResponse = jo.getJsonObject("instance_record");
            jo.put(PARSED_RECORD, instanceRecordFromViewResponse.getJsonObject(PARSED_RECORD));
          }
        } else {
          JsonObject content = new JsonObject();
          content.put("content", marcRecord);
          jo.remove(MARC_RECORD_FROM_VIEW_RESPONSE);
          jo.put(PARSED_RECORD, content);
        }
        updatedWithParsedRecord.add(jo);
      });

    logger.debug("Execution of buildResult: {} sec", (System.nanoTime() - t) / 1_000_000_000);
    logger.info("End of buildResult, result size: {}", updatedWithParsedRecord.size());
    promise.complete(updatedWithParsedRecord);
  }

  @Override
  protected List<OAIPMHerrorType> validateRequest(Request request) {
    return validateListRequest(request);
  }

  @Autowired
  public void setInstancesService(InstancesService instancesService) {
    this.instancesService = instancesService;
  }

  @Override
  @Autowired
  public void setConsortiaService(ConsortiaService consortiaService) {
    this.consortiaService = consortiaService;
  }

  @Autowired
  public void setViewsService(ViewsService viewsService) {
    this.viewsService = viewsService;
  }

}
