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
import org.openarchives.oai._2.ListIdentifiersType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.folio.rest.jaxrs.resource.Oai.GetOaiIdentifiersResponse;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;

public class GetOaiIdentifiersHelper extends AbstractHelper {

  private static final Logger logger = Logger.getLogger(GetOaiIdentifiersHelper.class);
  private static final String GENERIC_ERROR = "Error happened while processing ListIdentifiers verb request";

  @Override
  public CompletableFuture<javax.ws.rs.core.Response> handle(Request request, Context ctx) {
    CompletableFuture<javax.ws.rs.core.Response> future = new VertxCompletableFuture<>(ctx);
    try {
      // 1. Validate request
      List<OAIPMHerrorType> errors = validateListRequest(request);
      if (!errors.isEmpty()) {
        OAIPMH oai = buildBaseResponse(request).withErrors(errors);
        future.complete(buildNoRecordsResponse(oai));
        return future;
      }

      HttpClientInterface httpClient = getOkapiClient(request.getOkapiHeaders());

      // 2. Search for instances
      httpClient.request(storageHelper.buildItemsEndpoint(request), request.getOkapiHeaders(), false)
        // 3. Verify response and build list of identifiers
        .thenApply(response -> buildListIdentifiers(request, response))
        .thenApply(identifiers -> {
          if (identifiers == null) {
            return buildNoRecordsResponse(buildNoRecordsFoundOaiResponse(request));
          } else {
            return buildSuccessResponse(buildBaseResponse(request)
              .withListIdentifiers(identifiers));
          }
        })
        .thenAccept(future::complete)
        .exceptionally(e -> {
          logger.error(GENERIC_ERROR, e);
          future.completeExceptionally(e);
          return null;
        });
    } catch (Exception e) {
      logger.error(GENERIC_ERROR, e);
      future.completeExceptionally(e);
    }

    return future;
  }

  private OAIPMH buildNoRecordsFoundOaiResponse(Request request) {
    return buildBaseResponse(request).withErrors(createNoRecordsFoundError());
  }

  private javax.ws.rs.core.Response buildNoRecordsResponse(OAIPMH oai) {
    String responseBody = ResponseHelper.getInstance().writeToString(oai);

    // According to oai-pmh.raml the service will return different http codes depending on the error
    Set<OAIPMHerrorcodeType> errorCodes = getErrorCodes(oai);
    if (errorCodes.contains(BAD_ARGUMENT) || errorCodes.contains(BAD_RESUMPTION_TOKEN)) {
      return GetOaiIdentifiersResponse.respond400WithApplicationXml(responseBody);
    } else if (errorCodes.contains(CANNOT_DISSEMINATE_FORMAT)) {
      return GetOaiIdentifiersResponse.respond422WithApplicationXml(responseBody);
    }
    return GetOaiIdentifiersResponse.respond404WithApplicationXml(responseBody);
  }

  private javax.ws.rs.core.Response buildSuccessResponse(OAIPMH oai) {
    return GetOaiIdentifiersResponse.respond200WithApplicationXml(ResponseHelper.getInstance().writeToString(oai));
  }

  /**
   * Builds {@link ListIdentifiersType} with headers if there is any item or {@code null}
   * @param request request
   * @param instancesResponse the response from the storage which contains items
   * @return {@link ListIdentifiersType} with headers if there is any or {@code null}
   */
  private ListIdentifiersType buildListIdentifiers(Request request, Response instancesResponse) {
    if (!Response.isSuccess(instancesResponse.getCode())) {
      logger.error("No instances found. Service responded with error: " + instancesResponse.getError());
      // The storage service could not return instances so we have to send 500 back to client
      throw new IllegalStateException(instancesResponse.getError().toString());
    }

    JsonArray instances = storageHelper.getItems(instancesResponse.getBody());
    if (instances != null && !instances.isEmpty()) {
      ListIdentifiersType identifiers = new ListIdentifiersType();
      String identifierPrefix = request.getIdentifierPrefix();
      instances.stream()
               .map(instance -> populateHeader(identifierPrefix, (JsonObject) instance))
               .forEach(identifiers::withHeaders);
      return identifiers;
    }
    return null;
  }
}
