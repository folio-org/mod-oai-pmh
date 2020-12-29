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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

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
import org.folio.rest.impl.SourceStorageSourceRecordsClient;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
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

import com.google.common.collect.Maps;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.parsetools.impl.JsonParserImpl;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.impl.Connection;


public class MarcWithHoldingsRequestHelper extends AbstractHelper {

  private static final int DATABASE_FETCHING_CHUNK_SIZE = 50;

  private static final String INSTANCE_ID_FIELD_NAME = "instanceId";

  public static final String ENRICHED_INSTANCE_ID = "instanceid";

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

  private InstancesService instancesService;

  public static MarcWithHoldingsRequestHelper getInstance() {
    return INSTANCE;
  }

  private MarcWithHoldingsRequestHelper() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  /**
   * Handle MarcWithHoldings request
   */
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

      String requestId;
      RequestMetadataLb requestMetadata = new RequestMetadataLb().setLastUpdatedDate(OffsetDateTime.now(ZoneId.systemDefault()));
      Future<RequestMetadataLb> updateRequestMetadataFuture;
      if (resumptionToken == null || request.getRequestId() == null) {
        requestId = UUID.randomUUID().toString();
        requestMetadata.setRequestId(UUID.fromString(requestId));
        updateRequestMetadataFuture = instancesService.saveRequestMetadata(requestMetadata, request.getTenant())
          .onFailure(th -> handleException(promise, th));
      } else {
        requestId = request.getRequestId();
        updateRequestMetadataFuture = instancesService.updateRequestMetadataByRequestId(requestId, requestMetadata, request.getTenant())
          .onFailure(th -> handleException(promise, th));
      }
      updateRequestMetadataFuture.compose(res -> {
        Promise<Void> fetchingIdsPromise = null;
        if (resumptionToken == null
          || request.getRequestId() == null) { // the first request from EDS
          /**
           * here the postgres client is not used any more, but at 'createBatchStream' method
           * we don't allow to write data faster then retrieving from response and such approach
           * should be integrated with jooq request
           */
          fetchingIdsPromise = createBatchStream(request, promise, vertxContext, requestId);
          fetchingIdsPromise.future().onComplete(e -> processBatch(request, vertxContext, promise, deletedRecordSupport, requestId, true));
        } else {
          processBatch(request, vertxContext, promise, deletedRecordSupport, requestId, false); //close client
        }
        return fetchingIdsPromise != null ? fetchingIdsPromise.future() : Future.succeededFuture();
      }).onFailure(th -> handleException(promise, th));
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  private void processBatch(Request request, Context context, Promise<Response> oaiPmhResponsePromise, boolean deletedRecordSupport, String requestId, boolean firstBatch) {
    try {

      int batchSize = Integer.parseInt(
        RepositoryConfigurationUtil.getProperty(request.getTenant(),
          REPOSITORY_MAX_RECORDS_PER_RESPONSE));

      getNextInstances(request, batchSize, context, requestId).future().onComplete(fut -> {
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
            .onSuccess(oaiPmhResponsePromise::complete)
            .onFailure(e -> handleException(oaiPmhResponsePromise, e));
          return;
        }

        String nextInstanceId = instances.size() < batchSize ? null : instances.get(batchSize).getString(INSTANCE_ID_FIELD_NAME);
        List<JsonObject> instancesWithoutLast = nextInstanceId != null ? instances.subList(0, batchSize) : instances;
        final SourceStorageSourceRecordsClient srsClient = new SourceStorageSourceRecordsClient(request.getOkapiUrl(),
          request.getTenant(), request.getOkapiToken());

        Future<Map<String, JsonObject>> srsResponse = Future.succeededFuture();
        if (CollectionUtils.isNotEmpty(instances)) {
          srsResponse = requestSRSByIdentifiers(srsClient, instancesWithoutLast, deletedRecordSupport);
        }
        srsResponse.onSuccess(res -> buildRecordsResponse(request, requestId, instancesWithoutLast, res,
          firstBatch, nextInstanceId, deletedRecordSupport).onSuccess(result -> {
          List<String> instanceIds = instancesWithoutLast.stream()
            .map(e -> e.getString(INSTANCE_ID_FIELD_NAME))
            .collect(toList());
          instancesService.deleteInstancesById(instanceIds, request.getTenant())
            .onComplete(r -> oaiPmhResponsePromise.complete(result)); //need remove this close client maybe
        }).onFailure(e -> handleException(oaiPmhResponsePromise, e)));
        srsResponse.onFailure(t -> handleException(oaiPmhResponsePromise, t));
      });
    } catch (Exception e) {
      handleException(oaiPmhResponsePromise, e);
    }
  }

  private Promise<List<JsonObject>> getNextInstances(Request request, int batchSize, Context context, String requestId) {
    Promise<List<JsonObject>> promise = Promise.promise();
    instancesService.getInstancesList(batchSize + 1, requestId, request.getTenant()).compose(instances -> {
      List<JsonObject> jsonInstances = instances.stream()
        .map(Instances::getJson)
        .map(JsonObject::new)
        .collect(Collectors.toList());
      return enrichInstances(jsonInstances, request, context);
    }).onFailure(throwable -> {
        promise.fail(throwable);
        logger.error("Cannot save ids: " + throwable.getMessage(), throwable.getCause());
      }
    ).onSuccess(promise::complete);
    return promise;
  }

  private Future<List<JsonObject>> enrichInstances(List<JsonObject> result, Request request, Context context) {
    Map<String, JsonObject> instances = result.stream().collect(toMap(e -> e.getString(INSTANCE_ID_FIELD_NAME), Function.identity()));
    Promise<List<JsonObject>> completePromise = Promise.promise();
    HttpClient httpClient = context.owner().createHttpClient();
    createInventoryPostRequest(httpClient, request)
      .onSuccess(httpClientRequest -> {
        httpClientRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
        httpClientRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
        httpClientRequest.putHeader(ACCEPT, APPLICATION_JSON);
        httpClientRequest.putHeader(CONTENT_TYPE, APPLICATION_JSON);

        BatchStreamWrapper databaseWriteStream = getBatchHttpStream(completePromise, context);
        JsonObject entries = new JsonObject();
        entries.put(INSTANCE_IDS_ENRICH_PARAM_NAME, new JsonArray(new ArrayList<>(instances.keySet())));
        entries.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS, isSkipSuppressed(request));
        httpClientRequest.send(entries.encode())
          .onSuccess(httpClientResponse -> writeResponseToStream(httpClient, completePromise, httpClientRequest,
            databaseWriteStream, httpClientResponse))
          .onFailure(e -> {
            logger.error(e.getMessage());
            completePromise.fail(e);
          });

        AtomicReference<ArrayDeque<Promise<Connection>>> queue = new AtomicReference<>();
        try {
          queue.set(getWaitersQueue(context.owner(), request));
        } catch (IllegalStateException ex) {
          logger.error(ex.getMessage());
          completePromise.fail(ex);
        }

        databaseWriteStream.setCapacityChecker(() -> queue.get().size() > 20);

        databaseWriteStream.handleBatch(batch -> {
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
      })
      .onFailure(e -> {
        logger.error(e.getMessage());
        completePromise.fail(e);
      });
    return completePromise.future();
  }

  private Future<HttpClientRequest> createInventoryPostRequest(HttpClient httpClient, Request request) {
    RequestOptions requestOptions = new RequestOptions();
    requestOptions.setAbsoluteURI(request.getOkapiUrl() + INVENTORY_INSTANCES_ENDPOINT);
    if(request.getOkapiUrl().contains("https")) {
      requestOptions.setSsl(true);
    }
    requestOptions.setMethod(HttpMethod.POST);
    return httpClient.request(requestOptions);
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

    List<RecordType> records =new ArrayList<>();
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
          logger.error("Error occurred while converting record to xml representation.", e, e.getMessage());
          logger.debug("Skipping problematic record due the conversion error. Source record id - " + storageHelper.getRecordId(srsInstance));
          return;
        }
      }
      if (filterInstance(request, srsInstance)) {
        records.add(record);
      }
    });
    return records;
  }

  private RecordType createRecord(Request request, JsonObject instance, String instanceId) {
    String identifierPrefix = request.getIdentifierPrefix();
    return new RecordType()
      .withHeader(createHeader(instance, request)
        .withIdentifier(getIdentifier(identifierPrefix, instanceId)));
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

    String inventoryQuery = format("%s?%s", INVENTORY_UPDATED_INSTANCES_ENDPOINT, params);
    logger.info("Sending request to : " + inventoryQuery);
    RequestOptions requestOptions = new RequestOptions();
    requestOptions.setAbsoluteURI(request.getOkapiUrl() + inventoryQuery);
    if(request.getOkapiUrl().contains("https")) {
      requestOptions.setSsl(true);
    }
    requestOptions.setMethod(HttpMethod.GET);
    return httpClient.request(requestOptions);
  }

  private void appendHeadersAndSetTimeout(Request request, HttpClientRequest httpClientRequest) {
    httpClientRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpClientRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpClientRequest.putHeader(ACCEPT, APPLICATION_JSON);
    httpClientRequest.setTimeout(REQUEST_TIMEOUT);
  }

  private boolean isSkipSuppressed(Request request) {
    return !getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
  }

  private Promise<Void> createBatchStream(Request request,
                                          Promise<Response> oaiPmhResponsePromise,
                                          Context vertxContext, String requestId) {
    Promise<Void> completePromise = Promise.promise();
    final HttpClientOptions options = new HttpClientOptions();
    options.setKeepAliveTimeout(REQUEST_TIMEOUT);
    options.setConnectTimeout(REQUEST_TIMEOUT);
    options.setIdleTimeout(REQUEST_TIMEOUT);
    HttpClient httpClient = vertxContext.owner().createHttpClient(options);
    buildInventoryQuery(httpClient, request)
      .onSuccess(httpClientRequest -> {
        appendHeadersAndSetTimeout(request, httpClientRequest);
        BatchStreamWrapper databaseWriteStream = getBatchHttpStream(oaiPmhResponsePromise, vertxContext);
        httpClientRequest.exceptionHandler(e -> {
          logger.error(e.getMessage(), e);
          handleException(oaiPmhResponsePromise, e);
        });
        httpClientRequest.send()
          .onSuccess(response -> writeResponseToStream(httpClient, oaiPmhResponsePromise, httpClientRequest,
            databaseWriteStream, response))
          .onFailure(e -> {
            logger.error(e.getMessage());
            oaiPmhResponsePromise.fail(e);
          });
        AtomicReference<ArrayDeque<Promise<Connection>>> queue = new AtomicReference<>();
        try {
          queue.set(getWaitersQueue(vertxContext.owner(), request));
        } catch (IllegalStateException ex) {
          logger.error(ex.getMessage());
          oaiPmhResponsePromise.fail(ex);
        }

        databaseWriteStream.setCapacityChecker(() -> queue.get().size() > 20);

        databaseWriteStream.handleBatch(batch -> {
          Promise<Void> savePromise = saveInstancesIds(batch, request, requestId, databaseWriteStream);
          final Long returnedCount = databaseWriteStream.getReturnedCount();

          if (returnedCount % 1000 == 0) {
            logger.info("Batch saving progress: " + returnedCount + " returned so far, batch size: " + batch.size() + ", http ended: " + databaseWriteStream.isStreamEnded());
          }

          if (databaseWriteStream.isTheLastBatch()) {
            savePromise.future().toCompletionStage().thenRun(completePromise::complete);
          }
          databaseWriteStream.invokeDrainHandler();
        });
      })
      .onFailure(e -> {
        logger.error(e.getMessage());
        oaiPmhResponsePromise.fail(e);
      });
    return completePromise;
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

  private ArrayDeque<Promise<Connection>> getWaitersQueue(Vertx vertx, Request request) {
    PgPool pgPool = PostgresClientFactory.getPool(vertx, request.getTenant());
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

  private BatchStreamWrapper getBatchHttpStream(Promise<?> promise, Context vertxContext) {
    final Vertx vertx = vertxContext.owner();
    BatchStreamWrapper databaseWriteStream = new BatchStreamWrapper(vertx, DATABASE_FETCHING_CHUNK_SIZE);
    databaseWriteStream.exceptionHandler(e -> {
      if (e != null) {
        handleException(promise, e);
      }
    });
    return databaseWriteStream;
  }

  private void writeResponseToStream(HttpClient inventoryHttpClient, Promise<?> promise, HttpClientRequest inventoryQuery, BatchStreamWrapper databaseWriteStream, HttpClientResponse resp) {
    if (resp.statusCode() != 200) {
      String errorMsg = getErrorFromStorageMessage("inventory-storage", inventoryQuery.absoluteURI(), resp.statusMessage());
      resp.bodyHandler(buffer -> logger.error(errorMsg + resp.statusCode() + "body: " + buffer.toString()));
      promise.fail(new IllegalStateException(errorMsg));
    } else {
      resp.bodyHandler(buffer -> logger.info("Response " + buffer));
      logger.info("Response " + resp);
      JsonParser jp = new JsonParserImpl(resp);
      jp.objectValueMode();
      jp.pipeTo(databaseWriteStream);
      jp.endHandler(e -> {
        databaseWriteStream.end();
        inventoryHttpClient.close();
      })
        .exceptionHandler(throwable -> {
          logger.error("Error has been occurred at JsonParser while reading data from response. Message:{0}", throwable.getMessage(), throwable);
          databaseWriteStream.end();
          inventoryHttpClient.close();
          promise.fail(throwable);
        });
    }
  }

  private Promise<Void> saveInstancesIds(List<JsonEvent> instances, Request request, String requestId, BatchStreamWrapper databaseWriteStream) {
    Promise<Void> promise = Promise.promise();
    List<Instances> instancesList = toInstancesList(instances, UUID.fromString(requestId));
    instancesService.saveInstances(instancesList, request.getTenant()).onComplete(res -> { //here NPE
      if (res.failed()) {
        logger.error("Cannot saving ids, error from database: " + res.cause().getMessage(), res.cause());
        promise.fail(res.cause());
      } else {
        logger.info("Save instances complete");
        promise.complete();
        databaseWriteStream.invokeDrainHandler();
      }
    });
    return promise;
  }

  private List<Instances> toInstancesList(List<JsonEvent> jsonEventInstances, UUID requestId) {
    return jsonEventInstances.stream().map(JsonEvent::objectValue).map(inst ->
      new Instances().setInstanceId(UUID.fromString(inst.getString(INSTANCE_ID_FIELD_NAME)))
        .setJson(inst.toString())
        .setRequestId(requestId)
    ).collect(Collectors.toList());
  }

  private Future<Map<String, JsonObject>> requestSRSByIdentifiers(SourceStorageSourceRecordsClient srsClient,
                                                                  List<JsonObject> batch, boolean deletedRecordSupport) {
    final List<String> listOfIds = extractListOfIdsForSRSRequest(batch);
    logger.info("Request to SRS: {0}", listOfIds);
    Promise<Map<String, JsonObject>> promise = Promise.promise();
    try {
      final Map<String, JsonObject> result = Maps.newHashMap();
      srsClient.postSourceStorageSourceRecords("INSTANCE", deletedRecordSupport, listOfIds)
          .onSuccess(srsResp -> {
          if (srsResp.statusCode() != 200) {
            String errorMsg = getErrorFromStorageMessage("source-record-storage", "/source-storage/source-records", srsResp.statusMessage());
            logger.error(errorMsg);
            promise.fail(new IllegalStateException(errorMsg));
          }
          try {
            final Object o = srsResp.bodyAsJsonObject();
            if (o instanceof JsonObject) {
              JsonObject entries = (JsonObject) o;
              final JsonArray records = entries.getJsonArray("sourceRecords");
              records.stream()
                .filter(Objects::nonNull)
                .map(JsonObject.class::cast)
                .forEach(jo -> result.put(jo.getJsonObject("externalIdsHolder").getString("instanceId"), jo));
            } else {
              logger.debug("Can't process response from SRS: {0}", srsResp.toString());
            }
            promise.complete(result);
          } catch (DecodeException ex) {
            String msg = "Invalid json has been returned from SRS, cannot parse response to json.";
            handleException(promise, new IllegalStateException(msg, ex));
          } catch (Exception e) {
            handleException(promise, e);
          }
        }
      )
      .onFailure(e -> {
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
