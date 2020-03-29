package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseHelper;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.openarchives.oai._2.MetadataType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static me.escoffier.vertx.completablefuture.VertxCompletableFuture.supplyBlockingAsync;
import static org.folio.oaipmh.Constants.GENERIC_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.LIST_ILLEGAL_ARGUMENTS_ERROR;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FLOW_ERROR;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

public abstract class AbstractGetRecordsHelper extends AbstractHelper {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public CompletableFuture<Response> handle(Request request, Context ctx) {
    CompletableFuture<Response> future = new VertxCompletableFuture<>(ctx);
    try {
      if (request.getResumptionToken() != null && !request.restoreFromResumptionToken()) {
        OAIPMH oai = buildBaseResponse(request)
          .withErrors(new OAIPMHerrorType().withCode(BAD_ARGUMENT).withValue(LIST_ILLEGAL_ARGUMENTS_ERROR));
        future.complete(buildResponseWithErrors(oai));
        return future;
      }

      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        OAIPMH oai = buildBaseResponse(request);
        if (request.isRestored()) {
          oai.withErrors(new OAIPMHerrorType().withCode(BAD_RESUMPTION_TOKEN).withValue(RESUMPTION_TOKEN_FORMAT_ERROR));
        } else {
          oai.withErrors(errors);
        }
        future.complete(buildResponseWithErrors(oai));
        return future;
      }

      final HttpClientInterface httpClient = getOkapiClient(request.getOkapiHeaders(), false);
      final String instanceEndpoint = storageHelper.buildRecordsEndpoint(request);

      logger.debug("Sending message to {}", instanceEndpoint);

      httpClient.request(instanceEndpoint, request.getOkapiHeaders(), false)
        .thenCompose(response -> buildRecordsResponse(ctx, httpClient, request, response))
        .thenAccept(value -> {
          httpClient.closeClient();
          future.complete(value);
        })
        .exceptionally(e -> {
          httpClient.closeClient();
          handleException(future, e);
          return null;
        });
    } catch (Exception e) {
      handleException(future, e);
    }
    return future;
  }

  private CompletableFuture<Response> buildNoRecordsFoundOaiResponse(OAIPMH oaipmh) {
    oaipmh.withErrors(createNoRecordsFoundError());
    return completedFuture(buildResponseWithErrors(oaipmh));
  }

  private CompletableFuture<MetadataType> getOaiMetadataByRecordId(Context ctx, HttpClientInterface httpClient, Request request, String id) {
    try {
      String metadataEndpoint = storageHelper.getRecordByIdEndpoint(id);
      logger.debug("Getting metadata info from {}", metadataEndpoint);

      return httpClient.request(metadataEndpoint, request.getOkapiHeaders(), false)
                       .thenCompose(response -> supplyBlockingAsync(ctx, () -> buildOaiMetadata(request, response)));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private CompletableFuture<Response> buildRecordsResponse(Context ctx, HttpClientInterface httpClient, Request request,
                                                           org.folio.rest.tools.client.Response instancesResponse) {
    requiresSuccessStorageResponse(instancesResponse);

    JsonObject body = instancesResponse.getBody();
    JsonArray instances = storageHelper.getItems(body);
    Integer totalRecords = storageHelper.getTotalRecords(body);

    logger.debug("{} entries retrieved out of {}", instances != null ? instances.size() : 0, totalRecords);

    // In case the request is based on resumption token, the response should be validated if no missed records since previous response
    if (request.isRestored() && !canResumeRequestSequence(request, totalRecords, instances)) {
      OAIPMH oaipmh = buildBaseResponse(request).withErrors(new OAIPMHerrorType()
        .withCode(BAD_RESUMPTION_TOKEN)
        .withValue(RESUMPTION_TOKEN_FLOW_ERROR));
      return completedFuture(buildResponseWithErrors(oaipmh));
    }

    ResumptionTokenType resumptionToken = buildResumptionToken(request, instances, totalRecords);

    /*
    * According to OAI-PMH guidelines: it is recommended that the responseDate reflect the time of the repository's clock at the start
    * of any database query or search function necessary to answer the list request, rather than when the output is written.
    */
    final OAIPMH oaipmh = buildBaseResponse(request);

    // In case the response is quite large, time to process might be significant. So running in worker thread to not block event loop
    return supplyBlockingAsync(ctx, () -> buildRecords(request, instances))
      .thenCompose(recordsMap -> {
        if (recordsMap.isEmpty()) {
          return buildNoRecordsFoundOaiResponse(oaipmh);
        } else {
          return updateRecordsWithoutMetadata(ctx, httpClient, request, recordsMap)
            .thenApply(records -> {
              addRecordsToOaiResponse(oaipmh, records);
              addResumptionTokenToOaiResponse(oaipmh, resumptionToken);
              return buildResponse(oaipmh);
            });
        }
      });
  }

  /**
   * Builds {@link Map} with storage id as key and {@link RecordType} with populated header if there is any,
   * otherwise empty map is returned
   */
  private Map<String, RecordType> buildRecords(Request request, JsonArray instances) {
    Map<String, RecordType> records = Collections.emptyMap();
    if (instances != null && !instances.isEmpty()) {
      // Using LinkedHashMap just to rely on order returned by storage service
      records = new LinkedHashMap<>();
      String identifierPrefix = request.getIdentifierPrefix();

      for (Object entity : instances) {
        JsonObject instance = (JsonObject) entity;

        String recordId = storageHelper.getRecordId(instance);
        String identifierId = storageHelper.getIdentifierId(instance);

        RecordType record = new RecordType()
          .withHeader(createHeader(instance)
          .withIdentifier(getIdentifier(identifierPrefix, identifierId)));

        // Some repositories like SRS can return record source data along with other info
        String source = storageHelper.getInstanceRecordSource(instance);
        if (source != null) {
          record.withMetadata(buildOaiMetadata(request, source));
        }
        records.put(recordId, record);
      }
    }
    return records;
  }

  /**
   * Builds {@link MetadataType} if the response from storage service is successful
   * @param request the request to get metadata prefix
   * @param sourceResponse the response with {@link JsonObject} which contains record metadata
   * @return OAI record metadata
   */
  private MetadataType buildOaiMetadata(Request request, org.folio.rest.tools.client.Response sourceResponse) {
    if (!org.folio.rest.tools.client.Response.isSuccess(sourceResponse.getCode())) {
      logger.error("Record not found. Service responded with error: " + sourceResponse.getError());

      // If no record found (404 status code), we need to skip such record for now (see MODOAIPMH-12)
      if (sourceResponse.getCode() == 404) {
        return null;
      }

      // the rest of the errors we cannot handle
      throw new IllegalStateException(sourceResponse.getError().toString());
    }

    String source = storageHelper.getRecordSource(sourceResponse.getBody());
    return buildOaiMetadata(request, source);
  }

  private MetadataType buildOaiMetadata(Request request, String content) {
    MetadataType metadata = new MetadataType();
    MetadataPrefix metadataPrefix = MetadataPrefix.fromName(request.getMetadataPrefix());
    byte[] byteSource = metadataPrefix.convert(content);
    Object record = ResponseHelper.getInstance().bytesToObject(byteSource);
    metadata.setAny(record);
    return metadata;
  }

  private CompletableFuture<Collection<RecordType>> updateRecordsWithoutMetadata(Context ctx, HttpClientInterface httpClient, Request request, Map<String, RecordType> records) {
    if (hasRecordsWithoutMetadata(records)) {
      List<CompletableFuture<Void>> cfs = new ArrayList<>();
      records.forEach((id, record) -> {
        if (Objects.isNull(record.getMetadata())) {
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
                  .filter(this::hasMetadata)
                  .map(Map.Entry::getValue)
                  .collect(Collectors.toList());
  }

  private boolean hasMetadata(Map.Entry<String, RecordType> entry) {
    boolean hasMetadata = Objects.nonNull(entry.getValue().getMetadata());
    if (!hasMetadata) {
      logger.warn(String.format("The record with '%s' storage's id has no metadata", entry.getKey()));
    }
    return hasMetadata;
  }

  /**
   * In case the storage service could not return instances we have to send 500 back to client.
   * So the method is intended to validate and throw {@link IllegalStateException} for failure responses
   * @param sourceResponse response from the storage
   */
  private void requiresSuccessStorageResponse(org.folio.rest.tools.client.Response sourceResponse) {
    if (!org.folio.rest.tools.client.Response.isSuccess(sourceResponse.getCode())) {
      logger.error("Source not found. Service responded with error: " + sourceResponse.getError());
      throw new IllegalStateException(sourceResponse.getError().toString());
    }
  }

  private void handleException(CompletableFuture<Response> future, Throwable e) {
    logger.error(GENERIC_ERROR_MESSAGE, e);
    future.completeExceptionally(e);
  }

  private javax.ws.rs.core.Response buildResponse(OAIPMH oai) {
    if (!oai.getErrors().isEmpty()) {
      return buildResponseWithErrors(oai);
    }
    return buildSuccessResponse(oai);
  }

  protected abstract Response buildSuccessResponse(OAIPMH oai);
  protected abstract Response buildResponseWithErrors(OAIPMH oai);
  protected abstract List<OAIPMHerrorType> validateRequest(Request request);
  protected abstract void addRecordsToOaiResponse(OAIPMH oaipmh, Collection<RecordType> records);
  protected abstract void addResumptionTokenToOaiResponse(OAIPMH oaipmh, ResumptionTokenType resumptionToken);

}
