package org.folio.oaipmh.helpers;

import com.google.common.io.Resources;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.helpers.referencedata.ReferenceDataProvider;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.helpers.referencedata.ReferenceData;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.processors.OaiPmhJsonParser;
import org.folio.oaipmh.service.MetricsCollectingService;
import org.folio.oaipmh.service.SourceStorageSourceRecordsClientWrapper;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.processor.RuleProcessor;
import org.folio.processor.referencedata.JsonObjectWrapper;
import org.folio.processor.referencedata.ReferenceDataWrapper;
import org.folio.processor.referencedata.ReferenceDataWrapperImpl;
import org.folio.processor.rule.Rule;
import org.folio.processor.translations.TranslationsFunctionHolder;
import org.folio.reader.EntityReader;
import org.folio.reader.JPathSyntaxEntityReader;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.spring.SpringContextUtil;
import org.folio.writer.RecordWriter;
import org.folio.writer.impl.JsonRecordWriter;
import org.openarchives.oai._2.ListIdentifiersType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.StatusType;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.VerbType;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.CONTENT;
import static org.folio.oaipmh.Constants.GENERIC_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.INSTANCE_ID_FIELD_NAME;
import static org.folio.oaipmh.Constants.INVENTORY_STORAGE;
import static org.folio.oaipmh.Constants.LOCATION;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.PARSED_RECORD;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_RECORDS_SOURCE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FLOW_ERROR;
import static org.folio.oaipmh.Constants.SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS;
import static org.folio.oaipmh.Constants.SRS_AND_INVENTORY;
import static org.folio.oaipmh.Constants.SUPPRESS_FROM_DISCOVERY;
import static org.folio.oaipmh.Constants.HTTPS;
import static org.folio.oaipmh.MetadataPrefix.MARC21WITHHOLDINGS;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getProperty;
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
  private static final String HOLDINGS_RECORD_ID = "holdingsRecordId";
  private static final String ID = "id";
  private static final String COPY_NUMBER = "copyNumber";

  private static final String ERROR_FROM_STORAGE = "Got error response from %s, uri: '%s' message: %s";
  private static final String ENRICH_INSTANCES_MISSED_PERMISSION = "Cannot get holdings and items due to lack of permission, permission required - inventory-storage.inventory-hierarchy.items-and-holdings.collection.post";
  private static final String GET_INSTANCE_BY_ID_INVALID_RESPONSE = "Cannot get instance by id %s. Status code: %s; status message: %s .";
  private static final String GET_INSTANCES_INVALID_RESPONSE = "Cannot get instances. Status code: %s; status message: %s .";
  private static final String CANNOT_GET_INSTANCE_BY_ID_REQUEST_ERROR = "Cannot get instance by id, instanceId - ";
  private static final String CANNOT_GET_INSTANCES_REQUEST_ERROR = "Cannot get instances";
  private static final String FAILED_TO_ENRICH_SRS_RECORD_ERROR = "Failed to enrich srs record with inventory data, srs record id - %s. Reason - %s";
  private static final String SKIPPING_PROBLEMATIC_RECORD_MESSAGE = "Skipping problematic record due the conversion error. Source record id - {}.";
  private static final String FAILED_TO_CONVERT_SRS_RECORD_ERROR = "Error occurred while converting record to xml representation. {}.";
  private static final String QUERY_TEMPLATE = "(source==FOLIO%s%s%s%s)";

  private final MetricsCollectingService metricsCollectingService = MetricsCollectingService.getInstance();
  private final RuleProcessor ruleProcessor = new RuleProcessor(TranslationsFunctionHolder.SET_VALUE);

  private static final Logger logger = LogManager.getLogger(AbstractGetRecordsHelper.class);

  private static final String DEFAULT_RULES_PATH = "rules/rulesDefault.json";

  private List<Rule> defaultRules;

  private ReferenceDataProvider referenceDataProvider;

  protected AbstractGetRecordsHelper() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public Future<Response> handle(Request request, Context ctx) {
    metricsCollectingService.startMetric(request.getRequestId(), SEND_REQUEST);
    Promise<Response> promise = Promise.promise();
    try {
      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, promise, errors);
      }
      logger.info("handle:: Process records from srs for requestId {}", request.getRequestId());
      requestAndProcessSrsRecords(request, ctx, promise, false);
    } catch (Exception e) {
      logger.error("handle:: Request failed for requestId {} with error {}", request.getRequestId(),  e.getMessage());
      handleException(promise, e);
    }
    return promise.future().onComplete(responseAsyncResult -> metricsCollectingService.endMetric(request.getRequestId(), SEND_REQUEST));
  }

  protected void handleInventoryResponse(AsyncResult<JsonObject> handler, Request request, Context ctx, Promise<Response> promise) {
    if (handler.succeeded()) {
      var inventoryRecords = handler.result();
      generateRecordsOnTheFly(request, inventoryRecords);
      processRecords(ctx, request, null, inventoryRecords)
        .onComplete(oaiResponse -> promise.complete(oaiResponse.result()));
    } else {
      logger.error("Request from inventory has been failed.", handler.cause());
      promise.fail(handler.cause());
    }
  }

  protected void requestAndProcessSrsRecords(Request request, Context ctx, Promise<Response> promise, boolean withInventory) {
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
       request.isFromInventory() ? 0 : batchSize + 1,
      getSrsRecordsBodyHandler(request, ctx, promise, withInventory, batchSize + 1));
  }

  protected void requestAndProcessInventoryRecords(Request request, Context ctx, Promise<Response> promise) {
    int batchSize = Integer.parseInt(
      RepositoryConfigurationUtil.getProperty(request.getRequestId(),
        REPOSITORY_MAX_RECORDS_PER_RESPONSE));
    requestFromInventory(request, batchSize + 1, request.getIdentifier() != null ? List.of(request.getStorageIdentifier()) : null, false, false).onComplete(handler -> {
      try {
        if (handler.succeeded()) {
          var inventoryRecords = handler.result();
          processRecords(ctx, request, null, inventoryRecords).onComplete(oaiResponse -> promise.complete(oaiResponse.result()));
        } else {
          String verbName = request.getVerb().value();
          logger.error("requestAndProcessInventoryRecords:: {} response from Inventory for requestId {}", verbName, request.getRequestId());
          throw new IllegalStateException(handler.cause());
        }
      } catch (DecodeException ex) {
        logger.error("requestAndProcessInventoryRecords:: Cannot parse response from inventory to json for requestId {}, errors message {}", request.getRequestId(), ex.getMessage());
        promise.fail(new IllegalStateException("Cannot parse response from inventory to json", ex));
      } catch (Exception ex) {
        logger.error("requestAndProcessInventoryRecords:: Exception getting {} for requestId {}, errors message {}", request.getVerb()
          .value(), request.getRequestId(), ex.getMessage());
        promise.fail(ex);
      }
    });
  }

  protected void generateRecordsOnTheFly(Request request, JsonObject inventoryRecords) {
    var instances = inventoryRecords.getJsonArray("instances");
    instances.forEach(item -> {
      var instance = new JsonObject();
      instance.put("instance", item);
      EntityReader entityReader = new JPathSyntaxEntityReader(instance.encode());
      RecordWriter recordWriter = new JsonRecordWriter();
      ReferenceData referenceData = referenceDataProvider.get(request);
      ReferenceDataWrapper referenceDataWrapper = getReferenceDataWrapper(referenceData);
      List<Rule> rules = getDefaultRulesFromFile();
      String processedRecord = ruleProcessor.process(entityReader, recordWriter, referenceDataWrapper, rules, (translationException ->
        logger.error("generateRecordsOnTheFly:: Exception occurred for requestId {} while mapping, exception: {}, inventory instance: {}", request.getRequestId(), translationException.getCause(), instance)));
      enrichWithParsedRecord((JsonObject) item, processedRecord);
    });
  }

  private void enrichWithParsedRecord(JsonObject instance, String marcRecord) {
    var parsedRecord = new JsonObject();
    parsedRecord.put("id", instance.getValue("id"));
    parsedRecord.put(CONTENT, new JsonObject(marcRecord));
    instance.put(PARSED_RECORD, parsedRecord);
  }

  private List<Rule> getDefaultRulesFromFile() {
    if (nonNull(defaultRules)) {
      return defaultRules;
    }
    URL url = Resources.getResource(DEFAULT_RULES_PATH);
    String stringRules;
    try {
      stringRules = Resources.toString(url, StandardCharsets.UTF_8);
    } catch (IOException e) {
      logger.error("Failed to fetch default rules for export");
      throw new NotFoundException(e);
    }
    defaultRules = new ArrayList<>();
    CollectionUtils.addAll(defaultRules, Json.decodeValue(stringRules, Rule[].class));
    return defaultRules;
  }

  private ReferenceDataWrapper getReferenceDataWrapper(ReferenceData referenceData) {
    if (referenceData == null) {
      return null;
    }
    Map<String, Map<String, JsonObjectWrapper>> referenceDataWrapper = referenceData.getReferenceData().entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, value -> new JsonObjectWrapper(value.getValue().getMap())))));
    return new ReferenceDataWrapperImpl(referenceDataWrapper);
  }

  private Handler<AsyncResult<HttpResponse<Buffer>>> getSrsRecordsBodyHandler(Request request, Context ctx,
      Promise<Response> promise, boolean withInventory, int limit) {
    return asyncResult -> {
      try {
        if (asyncResult.succeeded()) {
          HttpResponse<Buffer> response = asyncResult.result();
          if (isSuccess(response.statusCode())) {
            var srsRecords = response.bodyAsJsonObject();
            if (withInventory) {
              var numOfReturnedSrsRecords = srsRecords.getJsonArray("sourceRecords").size();
              if (numOfReturnedSrsRecords < limit && !request.isFromInventory()) {
                request.setOldSrsOffset(request.getOffset());
                request.setOffset(0);
                request.setInventoryOffsetShift(-numOfReturnedSrsRecords);
                request.setFromInventory(true);
              }
              requestFromInventory(request, limit - numOfReturnedSrsRecords, request.getIdentifier() != null ? List.of(request.getStorageIdentifier()) : null, false, false).onComplete(instancesHandler ->
                handleInventoryResponse(request, ctx, instancesHandler, srsRecords, promise));
            } else {
              processRecords(ctx, request, srsRecords, null).onComplete(oaiResponse -> promise.complete(oaiResponse.result()));
            }
          } else {
            String verbName = request.getVerb().value();
            String statusMessage = response.statusMessage();
            int statusCode = response.statusCode();
            logger.error("getSrsRecordsBodyHandler:: For requestId {} {} response from SRS status code: {}: {}",request.getRequestId(),  verbName, statusMessage, statusCode);
            throw new IllegalStateException(response.statusMessage());
          }
        } else {
          logger.error("getSrsRecordsBodyHandler:: Cannot obtain srs records for requestId {}. Got failed async result", request.getRequestId());
          promise.fail(new IllegalStateException("Cannot obtain srs records. Got failed async result.", asyncResult.cause()));
        }
      } catch (DecodeException ex) {
        logger.error("getSrsRecordsBodyHandler:: Invalid json from SRS, cannot parse it for requestId {}. Errors message {}", request.getRequestId(), ex.getMessage());
        promise.fail(new IllegalStateException("Invalid json has been returned from SRS, cannot parse response to json.", ex));
      } catch (Exception ex) {
        logger.error("getSrsRecordsBodyHandler:: For requestId {} exception getting {}, errors message {}", request.getRequestId(), request.getVerb()
          .value(), ex.getMessage());
        promise.fail(ex);
      }
    };
  }

  private void handleInventoryResponse(Request request, Context ctx, AsyncResult<JsonObject> instancesHandler,
                              JsonObject srsRecords, Promise<Response> promise) {
    if (instancesHandler.succeeded()) {
      var inventoryRecords = instancesHandler.result();

      // Case only for SRS+Inventory when record not found in SRS (see MODOAIPMH-224),
      // or verb is ListRecords (see MODOAIPMH-138).
      if (srsRecords.getJsonArray("sourceRecords").isEmpty() || request.getVerb() == VerbType.LIST_RECORDS) {
        generateRecordsOnTheFly(request, inventoryRecords);
      }
      processRecords(ctx, request, srsRecords, inventoryRecords)
        .onComplete(oaiResponse -> promise.complete(oaiResponse.result()));
    } else {
      logger.error("handleInventoryResponse:: For requestId {} Request to inventory failed with errors {}", request.getRequestId(), instancesHandler.cause().getMessage());
      promise.fail(instancesHandler.cause());
    }
  }

  protected Future<Response> processRecords(Context ctx, Request request,
                                    JsonObject srsRecords, JsonObject inventoryRecords) {
    JsonArray items = new JsonArray();
    Integer totalRecords = 0;

    if (nonNull(srsRecords)) {
      items.addAll(storageHelper.getItems(srsRecords));
      totalRecords = storageHelper.getTotalRecords(srsRecords);
    }
    if (nonNull(inventoryRecords)) {
      items.addAll(storageHelper.getItems(inventoryRecords));
      totalRecords += storageHelper.getTotalRecords(inventoryRecords);
      if (request.isFromInventory()) {
        request.setInventoryTotalRecords(storageHelper.getTotalRecords(inventoryRecords));
      }
    }

    var recordsSource = getProperty(request.getRequestId(), REPOSITORY_RECORDS_SOURCE);
    if (recordsSource.equals(SRS_AND_INVENTORY) && request.getTotalRecords() > totalRecords) {
      totalRecords = request.getTotalRecords();
    }

    logger.debug("{} entries retrieved out of {}.", items.size(), totalRecords);

    if (request.isRestored() && !canResumeRequestSequence(request, totalRecords, items)) {
      OAIPMH oaipmh = getResponseHelper().buildBaseOaipmhResponse(request).withErrors(new OAIPMHerrorType()
        .withCode(BAD_RESUMPTION_TOKEN)
        .withValue(RESUMPTION_TOKEN_FLOW_ERROR));
      Response response = getResponseHelper().buildFailureResponse(oaipmh, request);
      return Future.succeededFuture(response);
    }

    ResumptionTokenType resumptionToken = buildResumptionToken(request, items, totalRecords);

    /*
     * According to OAI-PMH guidelines: it is recommended that the responseDate reflect the time of the repository's clock at the start
     * of any database query or search function necessary to answer the list request, rather than when the output is written.
     */
    final OAIPMH oaipmh = getResponseHelper().buildBaseOaipmhResponse(request);
    Promise<Response> oaiResponsePromise = Promise.promise();
    buildRecords(ctx, request, items).onSuccess(recordsMap -> {
      Response response;
      if (recordsMap.isEmpty()) {
        response = buildNoRecordsFoundOaiResponse(oaipmh, request);
      } else {
        addRecordsToOaiResponse(oaipmh, recordsMap.values());
        addResumptionTokenToOaiResponse(oaipmh, resumptionToken);
        response = buildResponse(oaipmh, request);
      }
      oaiResponsePromise.complete(response);
    }).onFailure(throwable -> oaiResponsePromise.complete(buildNoRecordsFoundOaiResponse(oaipmh, request)));
    return oaiResponsePromise.future();
  }

  /**
   * Builds {@link Map} with storage id as key and {@link RecordType} with populated header if there is any,
   * otherwise empty map is returned
   */
  private Future<Map<String, RecordType>> buildRecords(Context context, Request request, JsonArray records) {
    Promise<Map<String, RecordType>> recordsPromise = Promise.promise();
    List<Future<JsonObject>> futures = new ArrayList<>();

    final boolean suppressedRecordsProcessingEnabled = getBooleanProperty(request.getRequestId(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

    Map<String, RecordType> recordsMap = new ConcurrentHashMap<>();

    if (records != null && !records.isEmpty()) {
      RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
      // Using LinkedHashMap just to rely on order returned by storage service
      records.stream()
        .map(JsonObject.class::cast)
        .filter(instance -> isNotEmpty(storageHelper.getIdentifierId(instance)))
        .forEach(jsonRecord -> {
          String recordId = storageHelper.getRecordId(jsonRecord);
          String instanceId = storageHelper.getIdentifierId(jsonRecord);
          RecordType recordType = createRecord(request, jsonRecord, instanceId);
          Future<JsonObject> enrichRecordFuture = enrichRecordIfRequired(request, jsonRecord, recordType, instanceId,
              suppressedRecordsProcessingEnabled).onSuccess(enrichedSrsRecord -> {
                // Some repositories like SRS can return record source data along with other info
                String source = storageHelper.getInstanceRecordSource(enrichedSrsRecord);
                if (source != null && recordType.getHeader().getStatus() == null) {
                  source = enrichSource(source, suppressedRecordsProcessingEnabled, metadataManager, enrichedSrsRecord, request);
                  try {
                    recordType.withMetadata(buildOaiMetadata(request, source));
                  } catch (Exception e) {
                    logger.error(FAILED_TO_CONVERT_SRS_RECORD_ERROR, e.getMessage(), e);
                    logger.debug(SKIPPING_PROBLEMATIC_RECORD_MESSAGE, recordId);
                    return;
                  }
                } else {
                  context.put(recordId, jsonRecord);
                }
                if (filterInstance(request, jsonRecord)) {
                  recordsMap.put(recordId, recordType);
                }
              })
                .onFailure(throwable -> {
                  String errorMsg = format(FAILED_TO_ENRICH_SRS_RECORD_ERROR, recordId, throwable.getMessage());
                  logger.error(errorMsg, throwable);
                });
          futures.add(enrichRecordFuture);
        });

      GenericCompositeFuture.all(futures).onComplete(res -> {
          if (res.succeeded()) {
            recordsPromise.complete(recordsMap);
          } else {
            recordsPromise.fail(res.cause());
          }
        });
      return recordsPromise.future();
    }
    recordsPromise.complete(recordsMap);
    return recordsPromise.future();
  }

  private String enrichSource(String source, boolean suppressedRecordsProcessingEnabled,
                                      RecordMetadataManager metadataManager, JsonObject enrichedSrsRecord, Request request) {
    if (suppressedRecordsProcessingEnabled) {
      source = metadataManager.updateMetadataSourceWithDiscoverySuppressedData(source, enrichedSrsRecord);
      if (request.getMetadataPrefix().equals(MARC21WITHHOLDINGS.getName())) {
        source = metadataManager.updateElectronicAccessFieldWithDiscoverySuppressedData(source, enrichedSrsRecord);
      }
    }
    return source;
  }

  protected RecordType createRecord(Request request, JsonObject srsRecord, String identifierId) {
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
      return requestFromInventory(request, 1, List.of(instanceId), false, false).compose(instance -> {
        JsonObject instanceRequiredFieldsOnly = new JsonObject();
        instanceRequiredFieldsOnly.put(INSTANCE_ID_FIELD_NAME, instanceId);
        instanceRequiredFieldsOnly.put(SUPPRESS_FROM_DISCOVERY, instance.getString("discoverySuppress"));
        return enrichInstances(Collections.singletonList(instanceRequiredFieldsOnly), request);
      })
        .compose(oneItemList -> Future.succeededFuture(oneItemList.iterator().next()))
        .compose(instanceWithHoldingsAndItems -> {
          RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
          boolean deletedRecordSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request.getRequestId());
          JsonObject updatedSrsWithItemsData = metadataManager.populateMetadataWithItemsData(srsRecordToEnrich,
              instanceWithHoldingsAndItems, shouldProcessSuppressedRecords);
          JsonObject updatedSrsRecord = metadataManager.populateMetadataWithHoldingsData(updatedSrsWithItemsData,
              instanceWithHoldingsAndItems, shouldProcessSuppressedRecords);
          if (deletedRecordSupport && storageHelper.isRecordMarkAsDeleted(updatedSrsRecord)) {
            recordType.getHeader().setStatus(StatusType.DELETED);
          }
          return Future.succeededFuture(updatedSrsRecord);
        });
    } else {
      return Future.succeededFuture(srsRecordToEnrich);
    }
  }

  protected Future<JsonObject> requestFromInventory(Request request, int limit, List<String> listOfIds, boolean ignoreDate,
                                                    boolean ignoreOffset) {
    final boolean deletedRecordsSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request.getRequestId());
    final boolean suppressedRecordsSupport = getBooleanProperty(request.getRequestId(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

    final Date updatedAfter = request.getFrom() == null || ignoreDate ? null : convertStringToDate(request.getFrom(), false, true);
    final Date updatedBefore = request.getUntil() == null || ignoreDate ? null : convertStringToDate(request.getUntil(), true, true);

    Promise<JsonObject> promise = Promise.promise();

    var queryId = nonNull(listOfIds) ? " and (id==" + String.join(" or id==", listOfIds) + ")" : EMPTY;
    var queryFrom = nonNull(updatedAfter) ?
      " and metadata.updatedDate>=" + DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(ZonedDateTime.ofInstant(updatedAfter.toInstant(), ZoneId.of("UTC"))) :
      EMPTY;
    var queryUntil = nonNull(updatedBefore) ?
      " and metadata.updatedDate<=" + DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(ZonedDateTime.ofInstant(updatedBefore.toInstant(), ZoneId.of("UTC"))) :
      EMPTY;
    var discoverySuppress = nonNull(deletedRecordsSupport ? null : suppressedRecordsSupport);
    var querySuppressFromDiscovery = discoverySuppress ? " and discoverySuppress==" + suppressedRecordsSupport :
      EMPTY;
    String query = "limit=" + limit + (ignoreOffset ? EMPTY : "&offset=" + request.getOffset()) + "&query=" +
      URLEncoder.encode(format(QUERY_TEMPLATE, queryId, queryFrom, queryUntil, querySuppressFromDiscovery), Charset.defaultCharset());
    String uri = request.getOkapiUrl() + INSTANCES_STORAGE_ENDPOINT + "?" + query;

    processRequest(uri, request, promise, listOfIds);

    return promise.future();
  }

  private void processRequest(String uri, Request request, Promise<JsonObject> promise, List<String> listOfIds) {
    var webClient = WebClientProvider.getWebClient();
    var httpRequest = webClient.getAbs(uri);
    if (request.getOkapiUrl().contains(HTTPS)) {
      httpRequest.ssl(true);
    }
    httpRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpRequest.putHeader(ACCEPT, APPLICATION_JSON);
    httpRequest.send().onSuccess(response -> {
        if (response.statusCode() == 200) {
          promise.complete(response.bodyAsJsonObject());
        } else {
          String errorMsg = nonNull(String.join(", ", listOfIds)) ?
            format(GET_INSTANCE_BY_ID_INVALID_RESPONSE, String.join(", ", listOfIds), response.statusCode(), response.statusMessage()) :
            format(GET_INSTANCES_INVALID_RESPONSE, response.statusCode(), response.statusMessage());
          promise.fail(new IllegalStateException(errorMsg));
        }
      })
      .onFailure(throwable -> {
        logger.error(nonNull(String.join(", ", listOfIds)) ? CANNOT_GET_INSTANCE_BY_ID_REQUEST_ERROR + String.join(", ", listOfIds) :
          CANNOT_GET_INSTANCES_REQUEST_ERROR, throwable);
        promise.fail(throwable);
      });
  }

  protected Future<List<JsonObject>> enrichInstances(List<JsonObject> instances, Request request) {
    Map<String, JsonObject> instancesMap = instances.stream()
      .collect(LinkedHashMap::new, (map, instance) -> map.put(instance.getString(INSTANCE_ID_FIELD_NAME), instance), Map::putAll);
    Promise<List<JsonObject>> promise = Promise.promise();
    var webClient = WebClientProvider.getWebClient();
    var httpRequest = webClient.postAbs(request.getOkapiUrl() + INVENTORY_ITEMS_AND_HOLDINGS_ENDPOINT);
    if (request.getOkapiUrl()
      .contains(HTTPS)) {
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
        updateItems(instance);
        addEffectiveLocationCallNumberFromHoldingsWithoutItems(instance);
      } else {
        logger.info("enrichInstances:: For requestId {} instance with instanceId {} wasn't in the request", request.getRequestId(), instanceId);
      }
    });
    jsonParser.exceptionHandler(throwable -> responseChecked.future().onSuccess(invalidResponseReceivedAndProcessed -> {
        if (invalidResponseReceivedAndProcessed) {
          return;
        }
        logger.error("enrichInstances:: For requestId {} error has been occurred at JsonParser for items-and-holdings response, errors {}", request.getRequestId(),  throwable.getMessage());
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
            logger.error("{}, status {}", errorFromStorageMessage, response.statusCode());
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

  /**
   * Builds {@link ListIdentifiersType} with headers if there is any item or {@code null}
   *
   * @param request           request
   * @param srsRecords the response from the SRS storage which contains items
   * @param inventoryRecords the response from the Inventory storage which contains items
   * @return {@link ListIdentifiersType} with headers if there is any or {@code null}
   */
  protected OAIPMH buildListIdentifiers(Request request, JsonObject srsRecords, JsonObject inventoryRecords) {
    ResponseHelper responseHelper = getResponseHelper();
    JsonArray instances = new JsonArray();
    Integer totalRecords = 0;
    if (nonNull(srsRecords)) {
      instances.addAll(storageHelper.getItems(srsRecords));
      totalRecords = storageHelper.getTotalRecords(srsRecords);
    }
    if (nonNull(inventoryRecords)) {
      instances.addAll(storageHelper.getItems(inventoryRecords));
      totalRecords += storageHelper.getTotalRecords(inventoryRecords);
      if (request.isFromInventory()) {
        request.setInventoryTotalRecords(storageHelper.getTotalRecords(inventoryRecords));
      }
    }

    var recordsSource = getProperty(request.getRequestId(), REPOSITORY_RECORDS_SOURCE);
    if (recordsSource.equals(SRS_AND_INVENTORY) && request.getTotalRecords() > totalRecords) {
      totalRecords = request.getTotalRecords();
    }

    if (request.isRestored() && !canResumeRequestSequence(request, totalRecords, instances)) {
      return responseHelper.buildOaipmhResponseWithErrors(request, BAD_RESUMPTION_TOKEN, RESUMPTION_TOKEN_FLOW_ERROR);
    }
    if (!instances.isEmpty()) {
      logger.debug("{} entries retrieved out of {}.", instances.size(), totalRecords);

      ListIdentifiersType identifiers = new ListIdentifiersType()
        .withResumptionToken(buildResumptionToken(request, instances, totalRecords));

      String identifierPrefix = request.getIdentifierPrefix();
      instances.stream()
        .map(JsonObject.class::cast)
        .filter(instance -> isNotEmpty(storageHelper.getIdentifierId(instance)))
        .filter(instance -> filterInstance(request, instance))
        .map(instance -> addHeader(identifierPrefix, request, instance))
        .forEach(identifiers::withHeaders);

      if (identifiers.getHeaders().isEmpty()) {
        OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request);
        return oaipmh.withErrors(createNoRecordsFoundError());
      }
      ResumptionTokenType resumptionToken = buildResumptionToken(request, instances, totalRecords);
      OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request).withListIdentifiers(identifiers);
      addResumptionTokenToOaiResponse(oaipmh, resumptionToken);
      return oaipmh;
    }
    return responseHelper.buildOaipmhResponseWithErrors(request, createNoRecordsFoundError());
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

  private void addEffectiveLocationCallNumberFromHoldingsWithoutItems(JsonObject instance) {
    JsonArray holdingsJson = instance.getJsonObject(ITEMS_AND_HOLDINGS_FIELDS)
      .getJsonArray(HOLDINGS);
    JsonArray itemsJson = instance.getJsonObject(ITEMS_AND_HOLDINGS_FIELDS)
      .getJsonArray(ITEMS);
    var excludeHoldingsIds = new HashSet<String>();
    for (Object item : itemsJson) {
      JsonObject itemJson = (JsonObject) item;
      excludeHoldingsIds.add(itemJson.getString(HOLDINGS_RECORD_ID));
    }
    for (Object holding : holdingsJson) {
      if (holding instanceof JsonObject) {
        JsonObject holdingJson = (JsonObject) holding;
        if (excludeHoldingsIds.contains(holdingJson.getString(ID))) continue;
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
        itemJson.put(RecordMetadataManager.INVENTORY_SUPPRESS_DISCOVERY_FIELD, Boolean.valueOf(holdingJson.getString(RecordMetadataManager.INVENTORY_SUPPRESS_DISCOVERY_FIELD)));
        itemJson.put(COPY_NUMBER, holdingJson.getString(COPY_NUMBER));
        itemsJson.add(itemJson);
      }
    }
  }

  private void updateItems(JsonObject instance) {
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

  @Autowired
  public void setReferenceDataProvider(ReferenceDataProvider referenceDataProvider) {
    this.referenceDataProvider = referenceDataProvider;
  }

}
