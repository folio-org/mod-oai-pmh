package org.folio.oaipmh.helpers;

import static io.vertx.core.http.HttpMethod.GET;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static me.escoffier.vertx.completablefuture.VertxCompletableFuture.supplyBlockingAsync;
import static org.folio.oaipmh.Constants.GENERAL_INFO_DATA_FIELD_INDEX_VALUE;
import static org.folio.oaipmh.Constants.GENERAL_INFO_DATA_FIELD_TAG_NUMBER;
import static org.folio.oaipmh.Constants.GENERIC_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.LIST_ILLEGAL_ARGUMENTS_ERROR;
import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FLOW_ERROR;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.SUPPRESS_FROM_DISCOVERY_SUBFIELD_CODE;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseConverter;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.helpers.streaming.BatchStreamWrapper;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.openarchives.oai._2.MetadataType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.StatusType;
import org.openarchives.oai._2.VerbType;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
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
import io.vertx.ext.web.RoutingContext;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;

//import org.folio.rest.client.SourceStorageClient;

public abstract class AbstractGetRecordsHelper extends AbstractHelper {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  private static final String FIELDS = "fields";
  private static final String INDEX_ONE = "ind1";
  private static final String INDEX_TWO = "ind2";
  private static final String SUBFIELDS = "subfields";

  protected final VerbType verbType;
  protected final Context vertxContext;
  protected final RoutingContext routingContext;

  protected AbstractGetRecordsHelper(VerbType verbType, Context vertxContext, RoutingContext routingContext) {
    this.verbType = verbType;
    this.vertxContext = vertxContext;
    this.routingContext = routingContext;
  }


  @Override
  public Future<Response> handle(Request request) {
    Promise<Response> oaiPmhResponsePromise = Promise.promise();
    try {
      if (request.getResumptionToken() != null && !request.restoreFromResumptionToken()) {
        ResponseHelper responseHelper = getResponseHelper();
        OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, BAD_ARGUMENT, LIST_ILLEGAL_ARGUMENTS_ERROR);
        oaiPmhResponsePromise.complete(responseHelper.buildFailureResponse(oaipmh, request));
        return oaiPmhResponsePromise.future();
      }

      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        ResponseHelper responseHelper = getResponseHelper();
        OAIPMH oai;
        if (request.isRestored()) {
          oai = responseHelper.buildOaipmhResponseWithErrors(request, BAD_RESUMPTION_TOKEN, RESUMPTION_TOKEN_FORMAT_ERROR);
        } else {
          oai = responseHelper.buildOaipmhResponseWithErrors(request, errors);
        }
        oaiPmhResponsePromise.complete(responseHelper.buildFailureResponse(oai, request));
        return oaiPmhResponsePromise.future();
      }
      //TODO MOVE HTTP CLIENTS TO OTHER CLASSES
      final Vertx vertx = vertxContext.owner();
      final HttpClient inventoryHttpClient = vertx.createHttpClient();
      final SourceStorageClient srsClient = new SourceStorageClient(request.getOkapiUrl(), request.getTenant(), request.getOkapiToken());

      //todo add params from request
      String inventoryEndpoint = "/oai-pmh-view/instances";
      final MultiMap edgeRequestParams = routingContext.request().params();
      final String inventoryQuery = buildInventoryQuery(inventoryEndpoint, request);
      logger.debug("Sending message to {}", inventoryQuery);
      final HttpClientRequest httpClientRequest = inventoryHttpClient.request(GET, inventoryQuery);

      //TODO WHAT IF NO RESUMPTION TOKEN
      //final UUID uuid = UUID.randomUUID();
      final Object o = vertxContext.get(request.getResumptionToken());
      if (o == null) {
        httpClientRequest.handler(resp -> {
          JsonParser jp = new JsonParserImpl(resp);
          jp.objectValueMode();
          final BatchStreamWrapper writeStream = new BatchStreamWrapper(vertx, new CopyOnWriteArrayList<>());
          jp.pipeTo(writeStream);
          jp.endHandler(v -> {
            vertxContext.remove(request.getResumptionToken());
            //TODO NO RETURN RESUMPTION TOKEN
          });
          vertxContext.put(request.getResumptionToken(), writeStream);
        });
      }
      final Object contextWriteStream = vertxContext.get(request.getResumptionToken());
      if (contextWriteStream instanceof BatchStreamWrapper) {
        BatchStreamWrapper writeStream = (BatchStreamWrapper) contextWriteStream;

        writeStream.handleBatch(mh -> {
          //TODO BATCH SIZE?
          final List<JsonEvent> batch = writeStream.getNext(100);

          final Future<Map<String, JsonObject>> mapFuture = requestSRSByIdentifiers(vertxContext, srsClient, request, batch);

          //TODO MERGE INVENTORY AND SRS RESPONSES


          //MAPPING TO OAI-PMH FIELDS

          //          responseFuture.compose(srsResponse->{
//
//
//
//          });


          //TODO RETURN TO THE CALLER
          //oaiPmhResponsePromise.complete(responseFuture);

        });
      }
//      inventoryHttpClient.request(instanceEndpoint, request.getOkapiHeaders(), false)
//        .thenCompose(response -> buildRecordsResponse(vertxContext, inventoryHttpClient, request, response))
//        .thenAccept(value -> {
//          inventoryHttpClient.closeClient();
//          future.complete(value);
//        })
//        .exceptionally(e -> {
//          inventoryHttpClient.closeClient();
//          handleException(future, e);
//          return null;
//        });
    } catch (Exception e) {
      handleException(oaiPmhResponsePromise.future(), e);
    }
    return oaiPmhResponsePromise.future();
  }

  private String buildInventoryQuery(String inventoryEndpoint, Request request) {

    Map<String, String> paramMap = new HashMap<>();
    final String from = request.getFrom();
    if (StringUtils.isNotEmpty(from)) {
      paramMap.put("startDate", from);
    }
    final String until = request.getUntil();
    if (StringUtils.isNotEmpty(until)) {
      paramMap.put("endDate", until);
    }
    paramMap.put("deletedRecordSupport",
      String.valueOf(RepositoryConfigurationUtil.isDeletedRecordsEnabled(request, REPOSITORY_DELETED_RECORDS)));
    paramMap.put("skipSuppressedFromDiscoveryRecords",
      String.valueOf(getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING)));

    final String params = paramMap.entrySet().stream()
      .map(e -> e.getKey() + "=" + e.getValue())
      .collect(Collectors.joining("&"));

    return String.format("%s?%s", inventoryEndpoint, params);
  }


  private CompletableFuture<Response> buildNoRecordsFoundOaiResponse(OAIPMH oaipmh, Request request) {
    oaipmh.withErrors(createNoRecordsFoundError());
    return completedFuture(getResponseHelper().buildFailureResponse(oaipmh, request));
  }

  private CompletableFuture<MetadataType> getOaiMetadataByRecordId(Context ctx, HttpClientInterface httpClient, Request request, String id) {
    try {
      String metadataEndpoint = storageHelper.getRecordByIdEndpoint(id);
      logger.debug("Getting metadata info from {}", metadataEndpoint);

      return httpClient.request(metadataEndpoint, request.getOkapiHeaders(), false)
        .thenCompose(response -> supplyBlockingAsync(ctx, () -> buildOaiMetadata(ctx, id, request, response)));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  //TODO RETURN TYPE
  private Future<Map<String, JsonObject>> requestSRSByIdentifiers(Context ctx, SourceStorageClient srsClient, Request request,
                                                                  List<JsonEvent> batch) {

    Promise<Map<String, JsonObject>> promise = Promise.promise();

    //todo go to srs
    //todo build query 'or'

    final String srsRequest = buildSrsRequest(batch);

    //TODO EMPTY RESPONSE?
    //TODO ERROR?
    try {
      srsClient.getSourceStorageRecords(srsRequest, 0, batch.size(), null, rh -> {


        rh.bodyHandler(bh -> {
          System.out.println("bh = " + bh);
        });

      });
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }


    return promise.future();
  }

  //todo join instanceIds with OR
  private String buildSrsRequest(List<JsonEvent> batch) {

    final StringBuilder request = new StringBuilder("(");

    for (JsonEvent jsonEvent : batch) {
      final Object aggregatedInstanceObject = jsonEvent.value();
      if (aggregatedInstanceObject instanceof JsonObject) {
        JsonObject instance = (JsonObject) aggregatedInstanceObject;
        final String identifier = instance.getString("identifier");
        request.append("externalIdsHolder.identifier=");
        request.append(identifier);
      }
    }
    request.append(")");
    return request.toString();
  }


  //TODO CHECK THIS IS NEEDED?
  private CompletableFuture<Response> buildRecordsResponse(Context ctx, HttpClientInterface httpClient, Request request,
                                                           org.folio.rest.tools.client.Response instancesResponse) {
    requiresSuccessStorageResponse(instancesResponse);

    JsonObject body = instancesResponse.getBody();
    JsonArray instances;
    if (RepositoryConfigurationUtil.isDeletedRecordsEnabled(request, REPOSITORY_DELETED_RECORDS)) {
      instances = storageHelper.getRecordsItems(body);
    } else {
      instances = storageHelper.getItems(body);
    }
    Integer totalRecords = storageHelper.getTotalRecords(body);

    logger.debug("{} entries retrieved out of {}", instances != null ? instances.size() : 0, totalRecords);

    // In case the request is based on resumption token, the response should be validated if no missed records since previous response
    if (request.isRestored() && !canResumeRequestSequence(request, totalRecords, instances)) {
      OAIPMH oaipmh = getResponseHelper().buildBaseOaipmhResponse(request).withErrors(new OAIPMHerrorType()  //
        .withCode(BAD_RESUMPTION_TOKEN)
        .withValue(RESUMPTION_TOKEN_FLOW_ERROR));
      return completedFuture(getResponseHelper().buildFailureResponse(oaipmh, request));
    }
    //TODO DO WE NEED RESUMPT8ION TOKEN?
    ResumptionTokenType resumptionToken = buildResumptionToken(request, instances, totalRecords);

    /*
     * According to OAI-PMH guidelines: it is recommended that the responseDate reflect the time of the repository's clock at the start
     * of any database query or search function necessary to answer the list request, rather than when the output is written.
     */
    final OAIPMH oaipmh = getResponseHelper().buildBaseOaipmhResponse(request);

    // In case the response is quite large, time to process might be significant. So running in worker thread to not block event loop
    return supplyBlockingAsync(ctx, () -> buildRecords(ctx, request, instances))
      .thenCompose(recordsMap -> {
        if (recordsMap.isEmpty()) {
          return buildNoRecordsFoundOaiResponse(oaipmh, request);
        } else {
          return updateRecordsWithoutMetadata(ctx, httpClient, request, recordsMap)
            .thenApply(records -> {
              addRecordsToOaiResponse(oaipmh, records);
              addResumptionTokenToOaiResponse(oaipmh, resumptionToken);
              return buildResponse(oaipmh, request);
            });
        }
      });
  }

  /**
   * Builds {@link Map} with storage id as key and {@link RecordType} with populated header if there is any,
   * otherwise empty map is returned
   */
  private Map<String, RecordType> buildRecords(Context context, Request request, JsonArray instances) {
    Map<String, RecordType> records = Collections.emptyMap();
    if (instances != null && !instances.isEmpty()) {
      // Using LinkedHashMap just to rely on order returned by storage service
      records = new LinkedHashMap<>();
      String identifierPrefix = request.getIdentifierPrefix();
      for (Object entity : instances) {
        JsonObject instance = (JsonObject) entity;
        String recordId;
        if (RepositoryConfigurationUtil.isDeletedRecordsEnabled(request, REPOSITORY_DELETED_RECORDS)) {
          recordId = storageHelper.getId(instance);
        } else {
          recordId = storageHelper.getRecordId(instance);
        }
        String identifierId = storageHelper.getIdentifierId(instance);
        if (StringUtils.isNotEmpty(identifierId)) {
          RecordType record = new RecordType()
            .withHeader(createHeader(instance)
              .withIdentifier(getIdentifier(identifierPrefix, identifierId)));
          if (RepositoryConfigurationUtil.isDeletedRecordsEnabled(request, REPOSITORY_DELETED_RECORDS) && storageHelper.isRecordMarkAsDeleted(instance)) {
            record.getHeader().setStatus(StatusType.DELETED);
          }
          // Some repositories like SRS can return record source data along with other info
          String source = storageHelper.getInstanceRecordSource(instance);
          if (source != null) {
            source = updateSourceWithDiscoverySuppressedDataIfNecessary(source, instance, request);
            if (record.getHeader().getStatus() == null) {
              record.withMetadata(buildOaiMetadata(request, source));
            }
          } else {
            context.put(recordId, instance);
          }
          if (filterInstance(request, instance)) {
            records.put(recordId, record);
          }
        }
      }
    }
    return records;
  }

  /**
   * Updates marc general info datafield(tag=999, ind1=ind2='f') with additional subfield which holds data about record discovery
   * suppression status. Additional subfield has code = 't' and value = '0' if record is discovery suppressed and '1' at opposite case.
   *
   * @param source      record source
   * @param sourceOwner record source owner
   * @param request     OAI-PMH request
   * @return record source
   */
  private String updateSourceWithDiscoverySuppressedDataIfNecessary(String source, JsonObject sourceOwner, Request request) {
    if (getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING)) {
      JsonObject content = new JsonObject(source);
      JsonArray fields = content.getJsonArray(FIELDS);
      Optional<JsonObject> generalInfoDataFieldOptional = getGeneralInfoDataField(fields);
      if (generalInfoDataFieldOptional.isPresent()) {
        updateDatafieldWithDiscoverySuppressedData(generalInfoDataFieldOptional.get(), sourceOwner);
      } else {
        appendGeneralInfoDatafieldWithDiscoverySuppressedData(fields, sourceOwner);
      }
      return content.encode();
    }
    return source;
  }

  private Optional<JsonObject> getGeneralInfoDataField(JsonArray fields) {
    return fields.stream()
      .map(obj -> (JsonObject) obj)
      .filter(jsonObject -> jsonObject.containsKey(GENERAL_INFO_DATA_FIELD_TAG_NUMBER))
      .filter(jsonObject -> {
        JsonObject entryOfJson = jsonObject.getJsonObject(GENERAL_INFO_DATA_FIELD_TAG_NUMBER);
        String ind1 = entryOfJson.getString(INDEX_ONE);
        String ind2 = entryOfJson.getString(INDEX_TWO);
        return StringUtils.isNotEmpty(ind1) && StringUtils.isNotEmpty(ind2) && ind1.equals(ind2)
          && ind1.equals(GENERAL_INFO_DATA_FIELD_INDEX_VALUE);
      })
      .findFirst();
  }

  @SuppressWarnings("unchecked")
  private void updateDatafieldWithDiscoverySuppressedData(JsonObject generalInfoDataField, JsonObject sourceOwner) {
    JsonObject innerJsonField = generalInfoDataField.getJsonObject(GENERAL_INFO_DATA_FIELD_TAG_NUMBER);
    JsonArray subfields = innerJsonField.getJsonArray(SUBFIELDS);
    Map<String, Object> map = new LinkedHashMap<>();
    int value = storageHelper.getSuppressedFromDiscovery(sourceOwner) ? 1 : 0;
    map.put("t", value);
    List<Object> list = subfields.getList();
    list.add(map);
  }

  @SuppressWarnings("unchecked")
  private void appendGeneralInfoDatafieldWithDiscoverySuppressedData(JsonArray fields, JsonObject sourceOwner) {
    List<Object> list = fields.getList();
    Map<String, Object> generalInfoDataField = new LinkedHashMap<>();
    Map<String, Object> generalInfoDataFieldContent = new LinkedHashMap<>();
    List<Object> subfields = new ArrayList<>();
    Map<String, Object> generalInfoDataFieldSubfield = new LinkedHashMap<>();

    int value = storageHelper.getSuppressedFromDiscovery(sourceOwner) ? 1 : 0;
    generalInfoDataFieldSubfield.put(SUPPRESS_FROM_DISCOVERY_SUBFIELD_CODE, value);
    subfields.add(generalInfoDataFieldSubfield);
    generalInfoDataFieldContent.put(INDEX_ONE, GENERAL_INFO_DATA_FIELD_INDEX_VALUE);
    generalInfoDataFieldContent.put(INDEX_TWO, GENERAL_INFO_DATA_FIELD_INDEX_VALUE);
    generalInfoDataFieldContent.put(SUBFIELDS, subfields);
    generalInfoDataField.put(GENERAL_INFO_DATA_FIELD_TAG_NUMBER, generalInfoDataFieldContent);
    list.add(generalInfoDataField);
  }

  /**
   * Builds {@link MetadataType} if the response from storage service is successful
   *
   * @param context        - holds json object that is a source owner which is used for building metadata
   * @param id             - source owner id
   * @param request        the request to get metadata prefix
   * @param sourceResponse the response with {@link JsonObject} which contains record metadata
   * @return OAI record metadata
   */
  private MetadataType buildOaiMetadata(Context context, String id, Request request, org.folio.rest.tools.client.Response sourceResponse) {
    if (!org.folio.rest.tools.client.Response.isSuccess(sourceResponse.getCode())) {
      logger.error("Record not found. Service responded with error: " + sourceResponse.getError());

      // If no record found (404 status code), we need to skip such record for now (see MODOAIPMH-12)
      if (sourceResponse.getCode() == 404) {
        return null;
      }

      // the rest of the errors we cannot handle
      throw new IllegalStateException(sourceResponse.getError().toString());
    }

    JsonObject sourceOwner = context.get(id);
    String source = storageHelper.getRecordSource(sourceResponse.getBody());
    source = updateSourceWithDiscoverySuppressedDataIfNecessary(source, sourceOwner, request);
    return buildOaiMetadata(request, source);
  }

  private MetadataType buildOaiMetadata(Request request, String content) {
    MetadataType metadata = new MetadataType();
    MetadataPrefix metadataPrefix = MetadataPrefix.fromName(request.getMetadataPrefix());
    byte[] byteSource = metadataPrefix.convert(content);
    Object record = ResponseConverter.getInstance().bytesToObject(byteSource);
    metadata.setAny(record);
    return metadata;
  }

  private CompletableFuture<Collection<RecordType>> updateRecordsWithoutMetadata(Context ctx, HttpClientInterface httpClient, Request request, Map<String, RecordType> records) {
    if (hasRecordsWithoutMetadata(records)) {
      List<CompletableFuture<Void>> cfs = new ArrayList<>();
      records.forEach((id, record) -> {
        if (Objects.isNull(record.getMetadata()) && record.getHeader().getStatus() == null) {
          cfs.add(getOaiMetadataByRecordId(ctx, httpClient, request, id).thenAccept(record::withMetadata));
        }
      });
      return VertxCompletableFuture.from(ctx, CompletableFuture.allOf(cfs.toArray(new CompletableFuture[0])))
        // Return only records with metadata populated
        .thenApply(v -> filterEmptyRecords(records));
    } else {
      return CompletableFuture.completedFuture(records.values());
    }
  }

  private boolean hasRecordsWithoutMetadata(Map<String, RecordType> records) {
    return records.values()
      .stream()
      .map(RecordType::getMetadata)
      .anyMatch(Objects::isNull);
  }

  private List<RecordType> filterEmptyRecords(Map<String, RecordType> records) {
    return records.entrySet()
      .stream()
      .map(Map.Entry::getValue)
      .collect(Collectors.toList());
  }

  /**
   * In case the storage service could not return instances we have to send 500 back to client.
   * So the method is intended to validate and throw {@link IllegalStateException} for failure responses
   *
   * @param sourceResponse response from the storage
   */
  private void requiresSuccessStorageResponse(org.folio.rest.tools.client.Response sourceResponse) {
    if (!org.folio.rest.tools.client.Response.isSuccess(sourceResponse.getCode())) {
      logger.error("Source not found. Service responded with error: " + sourceResponse.getError());
      throw new IllegalStateException(sourceResponse.getError().toString());
    }
  }

  private void handleException(Future<Response> future, Throwable e) {
    logger.error(GENERIC_ERROR_MESSAGE, e);
    future.fail(e);
  }

  private javax.ws.rs.core.Response buildResponse(OAIPMH oai, Request request) {
    if (!oai.getErrors().isEmpty()) {
      return getResponseHelper().buildFailureResponse(oai, request);
    }
    return getResponseHelper().buildSuccessResponse(oai);
  }

  protected abstract List<OAIPMHerrorType> validateRequest(Request request);

  protected abstract void addRecordsToOaiResponse(OAIPMH oaipmh, Collection<RecordType> records);

  protected abstract void addResumptionTokenToOaiResponse(OAIPMH oaipmh, ResumptionTokenType resumptionToken);

}
