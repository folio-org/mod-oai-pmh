package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;
import org.apache.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseHelper;
import org.folio.rest.tools.client.Response;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.RecordType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.folio.rest.jaxrs.resource.Oai.GetOaiRecordsResponse.respond200WithApplicationXml;
import static org.folio.rest.jaxrs.resource.Oai.GetOaiRecordsResponse.respond400WithApplicationXml;
import static org.folio.rest.jaxrs.resource.Oai.GetOaiRecordsResponse.respond404WithApplicationXml;
import static org.folio.rest.jaxrs.resource.Oai.GetOaiRecordsResponse.respond422WithApplicationXml;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;
import static org.openarchives.oai._2.VerbType.LIST_RECORDS;

public class GetOaiRecordsHelper extends AbstractHelper {

  private static final Logger logger = Logger.getLogger(GetOaiRecordsHelper.class);
  private static final String GENERIC_ERROR = "Error happened while processing ListRecords verb request";

  @Override
  public CompletableFuture<javax.ws.rs.core.Response> handle(Request request, Context ctx) {
    CompletableFuture<javax.ws.rs.core.Response> future = new VertxCompletableFuture<>(ctx);
    try {
      // 1. Validate request
      List<OAIPMHerrorType> errors = validateListRequest(request);
      if (!errors.isEmpty()) {
        OAIPMH oai = buildOaiResponse(request).withErrors(errors);
        future.complete(buildValidationFailureResponse(oai));
        return future;
      }

      HttpClientInterface httpClient = getOkapiClient(request.getOkapiHeaders());

      // 2. Search for instances
      httpClient.request(storageHelper.buildItemsEndpoint(request), request.getOkapiHeaders(), false)
        // 3. Verify response and build list of identifiers
        .thenApply(response -> buildRecords(request, response))
        .thenApply(records -> {
          if (records == null) {
            return buildNoRecordsFoundOaiResponse(request);
          } else {
            // 4. Now we need to get marc data for each instance and update record with metadata
            return buildSuccessResponse(buildOaiResponse(request)
              .withListRecords(new ListRecordsType().withRecords(records.values())));
          }
        })
        .thenAccept(future::complete)
        .exceptionally(e -> {
          future.completeExceptionally(e);
          return null;
        });
    } catch (Exception e) {
      logger.error(GENERIC_ERROR, e);
      future.completeExceptionally(e);
    }

    return future;
  }

  private javax.ws.rs.core.Response buildNoRecordsFoundOaiResponse(Request request) {
    OAIPMH oaipmh = buildOaiResponse(request).withErrors(createNoRecordsFoundError());
    return respond404WithApplicationXml(ResponseHelper.getInstance().writeToString(oaipmh));
  }

  private javax.ws.rs.core.Response buildValidationFailureResponse(OAIPMH oai) {
    String responseBody = ResponseHelper.getInstance().writeToString(oai);

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

  private javax.ws.rs.core.Response buildSuccessResponse(OAIPMH oai) {
    return respond200WithApplicationXml(ResponseHelper.getInstance().writeToString(oai));
  }

  private OAIPMH buildOaiResponse(Request request) {
    return buildBaseResponse(request.getOaiRequest().withVerb(LIST_RECORDS));
  }

  /**
   * Builds {{@link Map} if there are instances or {@code null}
   * @param instancesResponse the {@link JsonObject} which contains items
   * @return {@link Map} with storage id as key and {@link RecordType} with populated header if there is any or {@code null}
   */
  private Map<String, RecordType> buildRecords(Request request, Response instancesResponse) {
    if (!Response.isSuccess(instancesResponse.getCode())) {
      logger.error("No instances found. Service responded with error: " + instancesResponse.getError());
      // The storage service could not return instances so we have to send 500 back to client
      throw new IllegalStateException(instancesResponse.getError().toString());
    }

    JsonArray instances = storageHelper.getItems(instancesResponse.getBody());
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
}
