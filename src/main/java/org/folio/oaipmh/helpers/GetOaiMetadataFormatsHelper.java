package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.http.HttpMethod;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;
import org.apache.log4j.Logger;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseHelper;
import org.folio.rest.jaxrs.resource.Oai.GetOaiMetadataFormatsResponse;
import org.folio.rest.tools.client.Response;
import org.openarchives.oai._2.ListMetadataFormatsType;
import org.openarchives.oai._2.MetadataFormatType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;

import javax.xml.bind.JAXBException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.openarchives.oai._2.VerbType.LIST_METADATA_FORMATS;

public class GetOaiMetadataFormatsHelper extends AbstractHelper {

  private static final Logger logger = Logger.getLogger(GetOaiMetadataFormatsHelper.class);

  @Override
  public CompletableFuture<javax.ws.rs.core.Response> handle(Request request, Context ctx) {
    request.getOaiRequest().withVerb(LIST_METADATA_FORMATS);

    return retrieveMetadataFormats(request, ctx);
  }

  /**
   * Processes request with identifier
   * @return future with {@link OAIPMH} response
   */
  private CompletableFuture<javax.ws.rs.core.Response> retrieveMetadataFormats(Request request, Context ctx) {
    if (request.getIdentifier() == null) {
      return VertxCompletableFuture.completedFuture(retrieveMetadataFormats(request));
    }

    CompletableFuture<javax.ws.rs.core.Response> future = new VertxCompletableFuture<>(ctx);
    Map<String, String> okapiHeaders = request.getOkapiHeaders();
    try {
      getOkapiClient(okapiHeaders).request(HttpMethod.GET, "/instance-storage/instances/" + request.getIdentifier(), okapiHeaders)
        .thenApply(response -> verifyAndGetOaiPmhResponse(request, response))
        .thenAccept(future::complete)
        .exceptionally(t -> {
          future.completeExceptionally(t);
          return null;
        });
    } catch (Exception e) {
      logger.error("Error happened while processing ListMetadataFormats verb request", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  /**
   * Processes request without identifier
   * @return future with {@link OAIPMH} response
   */
  private javax.ws.rs.core.Response retrieveMetadataFormats(Request request) {
    return GetOaiMetadataFormatsResponse.respond200WithApplicationXml(buildMetadataFormatTypesResponse(request));
  }

  /**
   * Validates inventory-mod-storage response and returns {@link OAIPMH} with populated
   * MetadataFormatTypes or needed Errors according to OAI-PMH2 specification
   * @return basic {@link OAIPMH}
   */
  private javax.ws.rs.core.Response verifyAndGetOaiPmhResponse(Request request, Response response) {
    if (!Response.isSuccess(response.getCode())) {
      return GetOaiMetadataFormatsResponse.respond404WithApplicationXml(buildIdentifierNotFound(request));
    }
    return retrieveMetadataFormats(request);
  }

  /**
   * Creates {@link OAIPMH} with ListMetadataFormats element
   *
   * @return basic {@link OAIPMH}
   */
  private String buildMetadataFormatTypesResponse(Request request) {
    try {
      return ResponseHelper.getInstance()
                           .writeToString(buildBaseResponse(request.getOaiRequest())
                             .withListMetadataFormats(getMetadataFormatTypes()));
    } catch(JAXBException e) {
      logger.error("Error marshalling response: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates {@link OAIPMH} with Error id-does-not-exist
   * @return basic {@link OAIPMH}
   */
  private String buildIdentifierNotFound(Request request) {
    try {
      return ResponseHelper.getInstance()
        .writeToString(buildBaseResponse(request.getOaiRequest())
          .withErrors(new OAIPMHerrorType()
            .withValue("Identifier not found")
            .withCode(OAIPMHerrorcodeType.ID_DOES_NOT_EXIST)));
    } catch(JAXBException e) {
      logger.error("Error marshalling response: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates ListMetadataFormatsType of supported metadata formats
   * @return supported metadata formats
   */
  private ListMetadataFormatsType getMetadataFormatTypes() {
    ListMetadataFormatsType mft = new ListMetadataFormatsType();
    for (MetadataPrefix mp : MetadataPrefix.values()) {
      mft.withMetadataFormats(new MetadataFormatType()
        .withMetadataPrefix(mp.getName())
        .withSchema(mp.getSchema())
        .withMetadataNamespace(mp.getMetadataNamespace()));
    }
    return mft;
  }
}
