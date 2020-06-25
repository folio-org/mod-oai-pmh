package org.folio.oaipmh.helpers;

import static me.escoffier.vertx.completablefuture.VertxCompletableFuture.supplyBlockingAsync;
import static org.folio.oaipmh.Constants.GENERIC_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FLOW_ERROR;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.isDeletedRecordsEnabled;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.rest.client.SourceStorageSourceRecordsClient;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.openarchives.oai._2.MetadataType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.StatusType;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;

public abstract class AbstractGetRecordsHelper extends AbstractHelper {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public Future<Response> handle(Request request, Context ctx) {
    Promise<Response> promise = Promise.promise();
    try {
      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, promise, errors);
      }

      final SourceStorageSourceRecordsClient srsClient = new SourceStorageSourceRecordsClient(request.getOkapiUrl(),
        request.getTenant(), request.getOkapiToken());

      final boolean deletedRecordsSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request);
      final boolean suppressedRecordsSupport = getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

      final Date updatedAfter = request.getFrom() == null ? null : convertStringToDate(request.getFrom());
      final Date updatedBefore = request.getUntil() == null ? null : convertStringToDate(request.getUntil());

      int batchSize = Integer.parseInt(
        RepositoryConfigurationUtil.getProperty(request.getTenant(),
          REPOSITORY_MAX_RECORDS_PER_RESPONSE));

      //source-storage/records?query=recordType%3D%3DMARC+and+metadata.updatedDate%3E%3D3999-01-01T00%3A00%3A00.000Z&limit=51&offset=0
      srsClient.getSourceStorageSourceRecords(
        null,
        null,
        null,
        "MARC",
        //NULL if we want suppressed and not suppressed, TRUE = ONLY SUPPRESSED FALSE = ONLY NOT SUPPRESSED
        !suppressedRecordsSupport ? suppressedRecordsSupport : null,
        deletedRecordsSupport,
        null,
        updatedAfter,
        updatedBefore,
        null,
        request.getOffset(),
        batchSize,
        response -> {
          try {
            if (org.folio.rest.tools.client.Response.isSuccess(response.statusCode())) {
              response.bodyHandler(bh -> {
                final Response responseCompletableFuture = buildRecordsResponse(ctx, request, bh.toJsonObject());
                promise.complete(responseCompletableFuture);
              });
            } else {
              logger.error("ListRecords response from SRS status code: {}: {}", response.statusMessage(), response.statusCode());
              throw new IllegalStateException(response.statusMessage());
            }
          } catch (Exception e) {
            logger.error("Exception getting ListRecords", e);
            promise.fail(e);
          }
        });

       //region REMOVE IT
//      httpClient.request(instanceEndpoint, request.getOkapiHeaders(), false)
//        .thenCompose(response -> buildRecordsResponse(ctx, httpClient, request, response))
//        .thenAccept(value -> {
//          httpClient.closeClient();
//          promise.complete(value);
//        })
//        .exceptionally(e -> {
//          httpClient.closeClient();
//          handleException(promise, e);
//          return null;
//        });
      //endregion
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  private Response buildNoRecordsFoundOaiResponse(OAIPMH oaipmh, Request request) {
    oaipmh.withErrors(createNoRecordsFoundError());
    return getResponseHelper().buildFailureResponse(oaipmh, request);
  }

  // TODO: HttpClientInstance occurrence. Need changes.
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


  private Response buildRecordsResponse(Context ctx, Request request,
                                                           JsonObject instancesResponseBody) {
    JsonArray instances = storageHelper.getItems(instancesResponseBody);
    Integer totalRecords = storageHelper.getTotalRecords(instancesResponseBody);

    logger.debug("{} entries retrieved out of {}", instances != null ? instances.size() : 0, totalRecords);

    // In case the request is based on resumption token, the response should be validated if no missed records since previous response
    if (request.isRestored() && !canResumeRequestSequence(request, totalRecords, instances)) {
      OAIPMH oaipmh = getResponseHelper().buildBaseOaipmhResponse(request).withErrors(new OAIPMHerrorType()  //
        .withCode(BAD_RESUMPTION_TOKEN)
        .withValue(RESUMPTION_TOKEN_FLOW_ERROR));
      return getResponseHelper().buildFailureResponse(oaipmh, request);
    }

    ResumptionTokenType resumptionToken = buildResumptionToken(request, instances, totalRecords);

    /*
     * According to OAI-PMH guidelines: it is recommended that the responseDate reflect the time of the repository's clock at the start
     * of any database query or search function necessary to answer the list request, rather than when the output is written.
     */
    final OAIPMH oaipmh = getResponseHelper().buildBaseOaipmhResponse(request);
    final Map<String, RecordType> recordsMap = buildRecords(ctx, request, instances);
    if (recordsMap.isEmpty()) {
      return buildNoRecordsFoundOaiResponse(oaipmh, request);
    } else {
      addRecordsToOaiResponse(oaipmh, recordsMap.values());
      addResumptionTokenToOaiResponse(oaipmh, resumptionToken);
      return buildResponse(oaipmh, request);
    }
    //TODO REMOVE IT
//    return supplyBlockingAsync(ctx, () -> buildRecords(ctx, request, instances))
//      .thenCompose(recordsMap -> {
//        if (recordsMap.isEmpty()) {
//          return buildNoRecordsFoundOaiResponse(oaipmh, request);
//        } else {
//
//          return updateRecordsWithoutMetadata(ctx, null, request, recordsMap)
//            .thenApply(records -> {
//              addRecordsToOaiResponse(oaipmh, records);
//              addResumptionTokenToOaiResponse(oaipmh, resumptionToken);
//              return buildResponse(oaipmh, request);
//            });
//        }
//      });
  }

  /**
   * Builds {@link Map} with storage id as key and {@link RecordType} with populated header if there is any,
   * otherwise empty map is returned
   */
  private Map<String, RecordType> buildRecords(Context context, Request request, JsonArray instances) {
    Map<String, RecordType> records = Collections.emptyMap();
    if (instances != null && !instances.isEmpty()) {
      RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
      // Using LinkedHashMap just to rely on order returned by storage service
      records = new LinkedHashMap<>();
      String identifierPrefix = request.getIdentifierPrefix();
      for (Object entity : instances) {
        JsonObject instance = (JsonObject) entity;
        String recordId = storageHelper.getRecordId(instance);
        String identifierId = storageHelper.getIdentifierId(instance);
        if (StringUtils.isNotEmpty(identifierId)) {
          RecordType record = new RecordType()
            .withHeader(createHeader(instance)
              .withIdentifier(getIdentifier(identifierPrefix, identifierId)));
          if (isDeletedRecordsEnabled(request) && storageHelper.isRecordMarkAsDeleted(instance)) {
            record.getHeader().setStatus(StatusType.DELETED);
          }
          // Some repositories like SRS can return record source data along with other info
          String source = storageHelper.getInstanceRecordSource(instance);
          if (source != null) {
            source = metadataManager.updateMetadataSourceWithDiscoverySuppressedDataIfNecessary(source, instance, request);
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

    RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
    JsonObject sourceOwner = context.get(id);
    String source = storageHelper.getRecordSource(sourceResponse.getBody());
    source = metadataManager.updateMetadataSourceWithDiscoverySuppressedDataIfNecessary(source, sourceOwner, request);
    return buildOaiMetadata(request, source);
  }

  // TODO: HttpClientInstance occurrence. Need changes.
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

  private void handleException(Promise<Response> future, Throwable e) {
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
