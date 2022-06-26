package org.folio.oaipmh.helpers;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.processors.OaiPmhJsonParser;
import org.folio.oaipmh.service.MetricsCollectingService;
import org.folio.oaipmh.service.SourceStorageSourceRecordsClientWrapper;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.StatusType;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.lang.String.format;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.GENERIC_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.INSTANCE_ID_FIELD_NAME;
import static org.folio.oaipmh.Constants.INVENTORY_STORAGE;
import static org.folio.oaipmh.Constants.LOCATION;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FLOW_ERROR;
import static org.folio.oaipmh.Constants.SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS;
import static org.folio.oaipmh.Constants.SUPPRESS_FROM_DISCOVERY;
import static org.folio.oaipmh.MetadataPrefix.MARC21WITHHOLDINGS;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.isDeletedRecordsEnabled;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.CALL_NUMBER;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.HOLDINGS;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.ITEMS;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.NAME;
import static org.folio.oaipmh.service.MetricsCollectingService.MetricOperation.SEND_REQUEST;
import static org.folio.rest.tools.client.Response.isSuccess;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

public abstract class AbstractGetRecordsHelper extends AbstractHelper {

  private static final String INVENTORY_ITEMS_AND_HOLDINGS_ENDPOINT = "/inventory-hierarchy/items-and-holdings";
  private static final String INSTANCES_STORAGE_ENDPOINT = "/instance-storage/instances";

  private static final String INSTANCE_IDS_ENRICH_PARAM_NAME = "instanceIds";
  private static final String TEMPORARY_LOCATION = "temporaryLocation";
  private static final String PERMANENT_LOCATION = "permanentLocation";
  private static final String EFFECTIVE_LOCATION = "effectiveLocation";
  private static final String CODE = "code";

  private static final String ERROR_FROM_STORAGE = "Got error response from %s, uri: '%s' message: %s";
  private static final String ENRICH_INSTANCES_MISSED_PERMISSION = "Cannot get holdings and items due to lack of permission, permission required - inventory-storage.inventory-hierarchy.items-and-holdings.collection.post";
  private static final String GET_INSTANCE_BY_ID_INVALID_RESPONSE = "Cannot get instance by id %s. Status code: %s; status message: %s .";
  private static final String CANNOT_GET_INSTANCE_BY_ID_REQUEST_ERROR = "Cannot get instance by id, instanceId - ";
  private static final String FAILED_TO_ENRICH_SRS_RECORD_ERROR = "Failed to enrich srs record with inventory data, srs record id - %s. Reason - %s";
  private static final String SKIPPING_PROBLEMATIC_RECORD_MESSAGE = "Skipping problematic record due the conversion error. Source record id - {}.";
  private static final String FAILED_TO_CONVERT_SRS_RECORD_ERROR = "Error occurred while converting record to xml representation. {}.";

  private final MetricsCollectingService metricsCollectingService = MetricsCollectingService.getInstance();

  private static final Logger logger = LogManager.getLogger(AbstractGetRecordsHelper.class);

  @Override
  public Future<Response> handle(Request request, Context ctx) {
    metricsCollectingService.startMetric(request.getRequestId(), SEND_REQUEST);
    Promise<Response> promise = Promise.promise();
    try {
      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, promise, errors);
      }
      requestAndProcessSrsRecords(request, ctx, promise);
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future().onComplete(responseAsyncResult -> metricsCollectingService.endMetric(request.getRequestId(), SEND_REQUEST));
  }

  protected void requestAndProcessSrsRecords(Request request, Context ctx, Promise<Response> promise) {
    final var srsClient = new SourceStorageSourceRecordsClientWrapper(request.getOkapiUrl(),
      request.getTenant(), request.getOkapiToken(), WebClientProvider.getWebClient());

    final boolean deletedRecordsSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request.getRequestId());
    final boolean suppressedRecordsSupport = getBooleanProperty(request.getRequestId(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

    final Date updatedAfter = request.getFrom() == null ? null : convertStringToDate(request.getFrom(), false, true);
    final Date updatedBefore = request.getUntil() == null ? null : convertStringToDate(request.getUntil(), true, true);

    int batchSize = Integer.parseInt(
      RepositoryConfigurationUtil.getProperty(request.getRequestId(),
        REPOSITORY_MAX_RECORDS_PER_RESPONSE));
     srsClient.getSourceStorageSourceRecords(
      null,
      null,
      null,
      null,
      request.getIdentifier() != null ? request.getStorageIdentifier() : null,
      null,
      null,
      null,
      "MARC_BIB",
      //1. NULL if we want suppressed and not suppressed, TRUE = ONLY SUPPRESSED FALSE = ONLY NOT SUPPRESSED
      //2. use suppressed from discovery filtering only when deleted record support is enabled
      deletedRecordsSupport ? null : suppressedRecordsSupport,
      deletedRecordsSupport,
      null,
      updatedAfter,
      updatedBefore,
      null,
      request.getOffset(),
      batchSize + 1,
      getSrsRecordsBodyHandler(request, ctx, promise));
  }

  private Handler<AsyncResult<HttpResponse<Buffer>>> getSrsRecordsBodyHandler(Request request, Context ctx,
      Promise<Response> promise) {
    return asyncResult -> {
      try {
        if (asyncResult.succeeded()) {
          HttpResponse<Buffer> response = asyncResult.result();
          if (isSuccess(response.statusCode())) {
            var srsRecords = response.bodyAsJsonObject();
            final Response responseCompletableFuture = processRecords(ctx, request, srsRecords);
            promise.complete(responseCompletableFuture);
          } else {
            String verbName = request.getVerb().value();
            String statusMessage = response.statusMessage();
            int statusCode = response.statusCode();
            logger.error("{} response from SRS status code: {}: {}.", verbName, statusMessage, statusCode);
            throw new IllegalStateException(response.statusMessage());
          }
        } else {
          var msg = "Cannot obtain srs records. Got failed async result.";
          promise.fail(new IllegalStateException(msg, asyncResult.cause()));
        }
      } catch (DecodeException ex) {
        String msg = "Invalid json has been returned from SRS, cannot parse response to json.";
        logger.error(msg, ex);
        promise.fail(new IllegalStateException(msg, ex));
      } catch (Exception ex) {
        logger.error("Exception getting {}.", request.getVerb()
          .value(), ex);
        promise.fail(ex);
      }
    };
  }

  protected Response processRecords(Context ctx, Request request,
                                    JsonObject srsRecords) {
    JsonArray sourceRecords = storageHelper.getItems(srsRecords);
    Integer totalRecords = storageHelper.getTotalRecords(srsRecords);

    logger.debug("{} entries retrieved out of {}.", sourceRecords != null ? sourceRecords.size() : 0, totalRecords);

    if (request.isRestored() && !canResumeRequestSequence(request, totalRecords, sourceRecords)) {
      OAIPMH oaipmh = getResponseHelper().buildBaseOaipmhResponse(request).withErrors(new OAIPMHerrorType()
        .withCode(BAD_RESUMPTION_TOKEN)
        .withValue(RESUMPTION_TOKEN_FLOW_ERROR));
      return getResponseHelper().buildFailureResponse(oaipmh, request);
    }

    ResumptionTokenType resumptionToken = buildResumptionToken(request, sourceRecords, totalRecords);

    /*
     * According to OAI-PMH guidelines: it is recommended that the responseDate reflect the time of the repository's clock at the start
     * of any database query or search function necessary to answer the list request, rather than when the output is written.
     */
    final OAIPMH oaipmh = getResponseHelper().buildBaseOaipmhResponse(request);
    final Map<String, RecordType> recordsMap = buildRecords(ctx, request, sourceRecords);
    if (recordsMap.isEmpty()) {
      return buildNoRecordsFoundOaiResponse(oaipmh, request);
    } else {
      addRecordsToOaiResponse(oaipmh, recordsMap.values());
      addResumptionTokenToOaiResponse(oaipmh, resumptionToken);
      return buildResponse(oaipmh, request);
    }
  }

  /**
   * Builds {@link Map} with storage id as key and {@link RecordType} with populated header if there is any,
   * otherwise empty map is returned
   */
  private Map<String, RecordType> buildRecords(Context context, Request request, JsonArray srsRecords) {
    Promise<Map<String, RecordType>> promise = Promise.promise();
    CopyOnWriteArrayList<Future<Void>> futures = new CopyOnWriteArrayList<>();

    final boolean suppressedRecordsProcessingEnabled = getBooleanProperty(request.getRequestId(),
      REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

    if (srsRecords != null && !srsRecords.isEmpty()) {
      Map<String, RecordType> records = new ConcurrentHashMap<>();
      RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
      // Using LinkedHashMap just to rely on order returned by storage service
      srsRecords.stream()
        .map(JsonObject.class::cast)
        .filter(instance -> isNotEmpty(storageHelper.getIdentifierId(instance)))
        .forEach(srsRecord -> {
          String srsRecordId = storageHelper.getRecordId(srsRecord);
          String instanceId = storageHelper.getIdentifierId(srsRecord);
          RecordType record = createRecord(request, srsRecord, instanceId);
          enrichRecordIfRequired(request, srsRecord, record, instanceId, suppressedRecordsProcessingEnabled).onSuccess(enrichedSrsRecord -> {
            // Some repositories like SRS can return record source data along with other info
            String source = storageHelper.getInstanceRecordSource(enrichedSrsRecord);
            if (source != null && record.getHeader().getStatus() == null) {
              if (suppressedRecordsProcessingEnabled) {
                source = metadataManager.updateMetadataSourceWithDiscoverySuppressedData(source, enrichedSrsRecord);
                if (request.getMetadataPrefix().equals(MARC21WITHHOLDINGS.getName())) {
                  source = metadataManager.updateElectronicAccessFieldWithDiscoverySuppressedData(source, enrichedSrsRecord);
                }
              }
              try {
                record.withMetadata(buildOaiMetadata(request, source));
              } catch (Exception e) {
                logger.error(FAILED_TO_CONVERT_SRS_RECORD_ERROR, e.getMessage(), e);
                logger.debug(SKIPPING_PROBLEMATIC_RECORD_MESSAGE, srsRecordId);
                return;
              }
            } else {
              context.put(srsRecordId, srsRecord);
            }
            if (filterInstance(request, srsRecord)) {
              records.put(srsRecordId, record);
            }
          }).onFailure(throwable -> {
            String errorMsg = format(FAILED_TO_ENRICH_SRS_RECORD_ERROR, srsRecordId, throwable.getMessage());
            logger.error(errorMsg, throwable);
          });
        });
      return records;
    }
    return Collections.emptyMap();
  }

  private RecordType createRecord(Request request, JsonObject srsRecord, String identifierId) {
    String identifierPrefix = request.getIdentifierPrefix();
    RecordType record = new RecordType()
      .withHeader(createHeader(srsRecord, request)
        .withIdentifier(getIdentifier(identifierPrefix, identifierId)));
    if (isDeletedRecordsEnabled(request.getRequestId()) && storageHelper.isRecordMarkAsDeleted(srsRecord)) {
      record.getHeader().setStatus(StatusType.DELETED);
    }
    return record;
  }

  private Response buildNoRecordsFoundOaiResponse(OAIPMH oaipmh, Request request) {
    oaipmh.withErrors(createNoRecordsFoundError());
    return getResponseHelper().buildFailureResponse(oaipmh, request);
  }

  protected javax.ws.rs.core.Response buildResponse(OAIPMH oai, Request request) {
    if (!oai.getErrors().isEmpty()) {
      return getResponseHelper().buildFailureResponse(oai, request);
    }
    return getResponseHelper().buildSuccessResponse(oai);
  }

  protected void handleException(Promise<?> promise, Throwable e) {
    logger.error(GENERIC_ERROR_MESSAGE, e);
    promise.fail(e);
  }

  protected abstract List<OAIPMHerrorType> validateRequest(Request request);

  private Future<JsonObject> enrichRecordIfRequired(Request request, JsonObject srsRecordToEnrich, RecordType recordType, String instanceId, boolean shouldProcessSuppressedRecords) {
    if (request.getMetadataPrefix().equals(MARC21WITHHOLDINGS.getName())) {
      return requestInstanceById(request, instanceId).compose(instance -> {
        JsonObject instanceRequiredFieldsOnly = new JsonObject();
        instanceRequiredFieldsOnly.put(INSTANCE_ID_FIELD_NAME, instanceId);
        instanceRequiredFieldsOnly.put(SUPPRESS_FROM_DISCOVERY, instance.getString("discoverySuppress"));
        return enrichInstances(Collections.singletonList(instanceRequiredFieldsOnly), request);
        })
        .compose(oneItemList -> Future.succeededFuture(oneItemList.iterator().next()))
        .compose(instanceWithHoldingsAndItems -> {
          RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
          boolean deletedRecordSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request.getRequestId());
            JsonObject updatedSrsWithItemsData = metadataManager.populateMetadataWithItemsData(srsRecordToEnrich, instanceWithHoldingsAndItems,
              shouldProcessSuppressedRecords);
            JsonObject updatedSrsRecord = metadataManager.populateMetadataWithHoldingsData(updatedSrsWithItemsData, instanceWithHoldingsAndItems,
              shouldProcessSuppressedRecords);
            if (deletedRecordSupport && storageHelper.isRecordMarkAsDeleted(updatedSrsRecord)) {
              recordType.getHeader()
                .setStatus(StatusType.DELETED);
            }
          return Future.succeededFuture(updatedSrsRecord);
        });
    } else {
      return Future.succeededFuture(srsRecordToEnrich);
    }
  }

  private Future<JsonObject> requestInstanceById(Request request, String instanceId) {
    Promise<JsonObject> promise = Promise.promise();
    var webClient = WebClientProvider.getWebClient();
    String uri = request.getOkapiUrl() + INSTANCES_STORAGE_ENDPOINT + "/" + instanceId;
    var httpRequest = webClient.getAbs(uri);
    if (request.getOkapiUrl().contains("https:")) {
      httpRequest.ssl(true);
    }
    httpRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpRequest.putHeader(ACCEPT, APPLICATION_JSON);
    httpRequest.send().onSuccess(response -> {
      if (response.statusCode() == 200) {
        promise.complete(response.bodyAsJsonObject());
      } else {
        String errorMsg = format(GET_INSTANCE_BY_ID_INVALID_RESPONSE, instanceId, response.statusCode(), response.statusMessage());
        promise.fail(new IllegalStateException(errorMsg));
      }
      })
      .onFailure(throwable -> {
        logger.error(CANNOT_GET_INSTANCE_BY_ID_REQUEST_ERROR + instanceId, throwable);
        promise.fail(throwable);
      });
    return promise.future();
  }

  protected Future<List<JsonObject>> enrichInstances(List<JsonObject> instances, Request request) {
    Map<String, JsonObject> instancesMap = instances.stream()
      .collect(LinkedHashMap::new, (map, instance) -> map.put(instance.getString(INSTANCE_ID_FIELD_NAME), instance), Map::putAll);
    Promise<List<JsonObject>> promise = Promise.promise();
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
    entries.put(INSTANCE_IDS_ENRICH_PARAM_NAME, new JsonArray(new ArrayList<>(instancesMap.keySet())));
    entries.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS, isSkipSuppressed(request));

    Promise<Boolean> responseChecked = Promise.promise();
    var jsonParser = new OaiPmhJsonParser()
      .objectValueMode();
    jsonParser.handler(event -> {
      JsonObject itemsAndHoldingsFields = event.objectValue();
      String instanceId = itemsAndHoldingsFields.getString(INSTANCE_ID_FIELD_NAME);
      JsonObject instance = instancesMap.get(instanceId);
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
    jsonParser.exceptionHandler(throwable -> responseChecked.future().onSuccess(invalidResponseReceivedAndProcessed -> {
        if (invalidResponseReceivedAndProcessed) {
          return;
        }
        logger.error("Error has been occurred at JsonParser while reading data from items-and-holdings response. Message:{}", throwable.getMessage(),
          throwable);
        promise.fail(throwable);
      })
    );

    httpRequest.as(BodyCodec.jsonStream(jsonParser))
      .sendBuffer(entries.toBuffer())
      .onSuccess(response -> {
        switch (response.statusCode()) {
          case 200:
            responseChecked.complete(false);
            break;
          case 403: {
            String errorMsg = getErrorFromStorageMessage(INVENTORY_STORAGE, request.getOkapiUrl() + INVENTORY_ITEMS_AND_HOLDINGS_ENDPOINT, ENRICH_INSTANCES_MISSED_PERMISSION);
            logger.error(errorMsg);
            promise.fail(new IllegalStateException(errorMsg));
            responseChecked.complete(true);
            break;
          }
          default: {
            String errorFromStorageMessage = getErrorFromStorageMessage(INVENTORY_STORAGE,
              request.getOkapiUrl() + INVENTORY_ITEMS_AND_HOLDINGS_ENDPOINT, response.statusMessage());
            String errorMessage = errorFromStorageMessage + response.statusCode();
            logger.error(errorMessage);
            promise.fail(new IllegalStateException(errorFromStorageMessage));
            responseChecked.complete(true);
          }
        }
        promise.complete(new ArrayList<>(instancesMap.values()));
      })
      .onFailure(e -> {
        logger.error(e.getMessage());
        promise.fail(e);
      });
    return promise.future();
  }

  protected boolean isSkipSuppressed(Request request) {
    return !getBooleanProperty(request.getRequestId(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
  }

  private void enrichDiscoverySuppressed(JsonObject itemsandholdingsfields, JsonObject instance) {
    if (Boolean.parseBoolean(instance.getString(SUPPRESS_FROM_DISCOVERY)))
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

  protected String getErrorFromStorageMessage(String errorSource, String uri, String responseMessage) {
    return format(ERROR_FROM_STORAGE, errorSource, uri, responseMessage);
  }

  protected void addRecordsToOaiResponse(OAIPMH oaipmh, Collection<RecordType> records) {
    if (!records.isEmpty()) {
      logger.debug("{} records found for the request.", records.size());
      oaipmh.withListRecords(new ListRecordsType().withRecords(records));
    } else {
      oaipmh.withErrors(createNoRecordsFoundError());
    }
  }

  protected void addResumptionTokenToOaiResponse(OAIPMH oaipmh, ResumptionTokenType resumptionToken) {
    throw new NotImplementedException();
  }

}
