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
import org.openarchives.oai._2.ListIdentifiersType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;

import javax.xml.bind.JAXBException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.folio.rest.jaxrs.resource.Oai.GetOaiIdentifiersResponse.respond200WithApplicationXml;
import static org.folio.rest.jaxrs.resource.Oai.GetOaiIdentifiersResponse.respond404WithApplicationXml;
import static org.openarchives.oai._2.VerbType.LIST_IDENTIFIERS;

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
        OAIPMH oai = buildOaiResponse(request).withErrors(errors);
        future.complete(buildNoRecordsResponse(oai));
        return future;
      }

      HttpClientInterface httpClient = getOkapiClient(request.getOkapiHeaders());

      // 2. Search for instances
      httpClient.request(getEndpoint(request), request.getOkapiHeaders(), false)
        // 3. Verify response and build list of identifiers
        .thenApply(response -> buildListIdentifiers(request, response))
        .thenApply(identifiers -> {
          if (identifiers == null) {
            return buildNoRecordsResponse(buildNoRecordsFoundOaiResponse(request));
          } else {
            return buildSuccessResponse(buildOaiResponse(request)
              .withListIdentifiers(identifiers));
          }
        })
        .thenAccept(future::complete)
        .exceptionally(e -> {
          future.completeExceptionally(e);
          return null;
        });
    } catch (Exception e) {
      handleException(future, e);
    }

    return future;
  }

  private OAIPMH buildNoRecordsFoundOaiResponse(Request request) {
    return buildOaiResponse(request).withErrors(createNoRecordsFoundError());
  }

  private javax.ws.rs.core.Response buildNoRecordsResponse(OAIPMH oai) {
    return respond404WithApplicationXml(getOaiResponseAsString(oai));
  }

  private javax.ws.rs.core.Response buildSuccessResponse(OAIPMH oai) {
    return respond200WithApplicationXml(getOaiResponseAsString(oai));
  }

  private String getOaiResponseAsString(OAIPMH oai) {
    try {
      return ResponseHelper.getInstance().writeToString(oai);
    } catch(JAXBException e) {
      logger.error("Error marshalling response: " + e.getMessage());
      // In case there is an issue to marshal response, there is no way to handle it
      throw new RuntimeException(e);
    }
  }

  private OAIPMH buildOaiResponse(Request request) {
    return buildBaseResponse(request.getOaiRequest().withVerb(LIST_IDENTIFIERS));
  }

  private void handleException(CompletableFuture<javax.ws.rs.core.Response> future, Throwable e) {
    logger.error(GENERIC_ERROR, e);
    future.completeExceptionally(e);
  }

  /**
   * Builds endpoint to search for the items
   * @param request {@link Request}
   * @return endpoint to use for the service call
   */
  private String getEndpoint(Request request) {
    request.getMetadataPrefix();
    return storageHelper.getItemsEndpoint();
  }

  /**
   * Builds {@link ListIdentifiersType} with headers if there is any item or {@code null}
   * @param instancesResponse the {@link JsonObject} which contains items
   * @return {@link ListIdentifiersType} with headers if there is any or {@code null}
   */
  private ListIdentifiersType buildListIdentifiers(Request request, Response instancesResponse) {
    if (!Response.isSuccess(instancesResponse.getCode())) {
      logger.error("No instances found. Service responded with error: " + instancesResponse.getError());
      return null;
    }

    JsonArray instances = storageHelper.getItems(instancesResponse.getBody());
    if (instances != null && !instances.isEmpty()) {
      ListIdentifiersType identifiers = new ListIdentifiersType();
      String tenantId = TenantTool.tenantId(request.getOkapiHeaders());
      instances.stream()
               .map(instance -> populateHeader(tenantId, (JsonObject) instance))
               .forEach(identifiers::withHeaders);
      return identifiers;
    }
    return null;
  }
}
