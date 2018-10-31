package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;
import org.apache.log4j.Logger;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseHelper;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.MetadataType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.RecordType;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.folio.rest.jaxrs.resource.Oai.GetOaiIdentifiersResponse.*;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.*;
import static org.openarchives.oai._2.VerbType.GET_RECORD;

public abstract class AbstractGetRecordsHelper extends AbstractHelper {


  private static final Logger logger = Logger.getLogger(AbstractGetRecordsHelper.class);

  private static final String GENERIC_ERROR = "Error happened while processing GetRecord verb " +
    "request";



  @Override
  public CompletableFuture<Response> handle(Request request, Context ctx) {
    CompletableFuture<Response> future = new VertxCompletableFuture<>(ctx);
    try {
      // 1. Validate request
      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        OAIPMH oai = buildOaiResponse(request).withErrors(errors);
        future.complete(buildNoRecordsResponse(oai));
        return future;
      }

      final HttpClientInterface httpClient = getOkapiClient(request.getOkapiHeaders());
      final String instanceEndpoint = storageHelper.buildItemsEndpoint(request);
      httpClient.request(instanceEndpoint, request.getOkapiHeaders(), false)
        .thenApply(response -> buildRecords(request, response))
        .thenCompose(records -> {
        if (records == null) {
          return buildNoRecordsFoundOaiResponse(request);
        } else {
          // 4. Now we need to get marc data for each instance and update record with metadata
          Map<String, String> okapiHeaders = request.getOkapiHeaders();
          HttpClientInterface sourceHttpClient = getOkapiClient(okapiHeaders);
          List<CompletableFuture<Void>> cfs = new ArrayList<>();
          records.forEach((id, record) -> cfs.add(buildOaiMetadata(sourceHttpClient, request,
            id).thenAccept(metadataType -> records.get(id).withMetadata(metadataType))));
          CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(cfs.toArray(new
            CompletableFuture[0]));
          return allDoneFuture.thenApply(v -> buildSuccessResponse(addRecordsToOaiResponce
            (buildOaiResponse(request), records.values())));
        }
        }).thenAccept(future::complete)
    .exceptionally(e -> {
        future.completeExceptionally(e);
        return null;
      });
  } catch (Exception e) {
    handleException(future, e);
  }
    return future;
}

  protected abstract OAIPMH addRecordsToOaiResponce(OAIPMH oaipmh, Collection<RecordType> records);

  private CompletableFuture<Response> buildNoRecordsFoundOaiResponse(Request request) {
    OAIPMH oaipmh = buildOaiResponse(request).withErrors(createNoRecordsFoundError());
    return VertxCompletableFuture.completedFuture(respond404WithApplicationXml(ResponseHelper
      .getInstance().writeToString(oaipmh)));
  }


  private CompletableFuture<MetadataType> buildOaiMetadata(HttpClientInterface
                                                         httpClient, Request request, String id) {
    try {
      return httpClient.request(getSourceEndpoint(id), request.getOkapiHeaders(), false)
        .thenApplyAsync(response -> buildOaiMetadata(request, response));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private Response buildNoRecordsResponse(OAIPMH oai) {
    String responseBody = getOaiResponseAsString(oai);

    // According to oai-pmh.raml the service will return different http codes depending on the error
    Set<OAIPMHerrorcodeType> errorCodes = oai.getErrors()
      .stream()
      .map(OAIPMHerrorType::getCode)
      .collect(Collectors.toSet());
    if (errorCodes.stream()
      .anyMatch(code -> (code == BAD_ARGUMENT || code == BAD_RESUMPTION_TOKEN))) {
      return respond400WithApplicationXml(responseBody);
    } else if (errorCodes.contains(CANNOT_DISSEMINATE_FORMAT)) {
      return respond422WithApplicationXml(responseBody);
    }
    return respond404WithApplicationXml(responseBody);
  }


  private String getOaiResponseAsString(OAIPMH oai) {
    return ResponseHelper.getInstance().writeToString(oai);
  }

  private OAIPMH buildOaiResponse(Request request) {
    return buildBaseResponse(request.getOaiRequest().withVerb(GET_RECORD));
  }

  private String getSourceEndpoint(String id) {
    return String.format("/instance-storage/instances/%s/source-record/marc-json", id);
  }

  private Map<String, RecordType> buildRecords(Request request, org.folio.rest.tools.client
    .Response sourceResponse) {
    if (!org.folio.rest.tools.client.Response.isSuccess(sourceResponse.getCode())) {
      logger.error("No instances found. Service responded with error: " + sourceResponse.getError());
      // The storage service could not return instances so we have to send 500 back to client
      throw new IllegalStateException(sourceResponse.getError().toString());
    }
    JsonArray instances = storageHelper.getItems(sourceResponse.getBody());
    if (instances != null && !instances.isEmpty()) {
      Map<String, RecordType> records = new HashMap<>();
      String tenantId = TenantTool.tenantId(request.getOkapiHeaders());
      String identifierPrefix = getIdentifierPrefix(tenantId, request.getIdentifierPrefix());
      for (Object entity : instances) {
        JsonObject instance = (JsonObject) entity;
        String id = storageHelper.getItemId(instance);
        RecordType record = new RecordType().withHeader(createHeader(instance).withIdentifier(identifierPrefix + id));
        records.put(id, record);
      }
      return records;
    }
    return null;
  }

  private MetadataType buildOaiMetadata(Request request, org.folio.rest.tools.client.Response
    sourceResponse) {
    if (!org.folio.rest.tools.client.Response.isSuccess(sourceResponse.getCode())) {
      logger.error("Source not found. Service responded with error: " + sourceResponse.getError());
      throw new IllegalStateException(sourceResponse.getError().toString());
    }
    JsonObject source = sourceResponse.getBody();
    MetadataType metadata = new MetadataType();
    MetadataPrefix metadataPrefix = MetadataPrefix.fromName(request.getMetadataPrefix());
    metadata.setAny(metadataPrefix.convert(source.toString()));
    return metadata;
  }

  private Response buildSuccessResponse(OAIPMH oai) {
    return respond200WithApplicationXml(getOaiResponseAsString(oai));
  }

  private void handleException(CompletableFuture<Response> future, Throwable e) {
    logger.error(GENERIC_ERROR, e);
    future.completeExceptionally(e);
  }

  abstract List<OAIPMHerrorType> validateRequest(Request request);

}
