package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.http.HttpMethod;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.xml.bind.JAXBException;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;
import org.apache.log4j.Logger;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseHelper;
import org.folio.rest.jaxrs.resource.Oai.GetOaiMetadataFormatsResponse;
import org.folio.rest.tools.client.Response;
import org.openarchives.oai._2.ListMetadataFormatsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.VerbType;

public class GetOaiMetadataFormatsHelper extends AbstractHelper {

  private static final Logger logger = Logger.getLogger(GetOaiMetadataFormatsHelper.class);

  @Override
  public CompletableFuture<javax.ws.rs.core.Response> handle(Request request, Context ctx) {
    String identifier = request.getIdentifier();
    Map<String, String> okapiHeaders = request.getOkapiHeaders();
    if (identifier != null) {
      return retrieveMetadataFormats(identifier, ctx, okapiHeaders);
    } else {
      return retrieveMetadataFormats(ctx);
    }
  }

  /**
   * Processes request with identifier
   * @return future with {@link OAIPMH} response
   */
  private CompletableFuture<javax.ws.rs.core.Response> retrieveMetadataFormats(String identifier, Context ctx, Map<String, String> okapiHeaders) {
    CompletableFuture<javax.ws.rs.core.Response> future = new VertxCompletableFuture<>(ctx);
    try {
      getHttpClient(okapiHeaders).request(HttpMethod.GET, "/instance-storage/instances/" + identifier, okapiHeaders)
        .thenApply(this::verifyAndGetOaiPmhResponse)
        .thenAccept(future::complete)
        .exceptionally(t -> {
          future.completeExceptionally(t);
          return null;
        });
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  /**
   * Processes request without identifier
   * @return future with {@link OAIPMH} response
   */
  private CompletableFuture<javax.ws.rs.core.Response> retrieveMetadataFormats(Context ctx) {
    CompletableFuture<javax.ws.rs.core.Response> future = new VertxCompletableFuture<>(ctx);
    try {
      future.complete(GetOaiMetadataFormatsResponse.respond200WithApplicationXml(buildMetadataFormatTypesResponse()));
    } catch (Exception e) {
      logger.error("Error happened while processing ListMetadataFormats verb request", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  /**
   * Validates inventory-mod-storage response and returns {@link OAIPMH} with populated
   * MetadataFormatTypes or needed Errors according to OAI-PMH2 specification
   * @return basic {@link OAIPMH}
   */
  private javax.ws.rs.core.Response verifyAndGetOaiPmhResponse(Response response) {
    try {
      if (!Response.isSuccess(response.getCode())) {
        return GetOaiMetadataFormatsResponse.respond404WithApplicationXml(buildIdentifierNotFound());
      }
      return GetOaiMetadataFormatsResponse.respond200WithApplicationXml(buildMetadataFormatTypesResponse());
    } catch(JAXBException e) {
      logger.error("Error marshalling response: " + e.getMessage());
    }
    return null;

  }

  /**
   * Creates {@link OAIPMH} with ListMetadataFormats element
   *
   * @return basic {@link OAIPMH}
   */
  private String buildMetadataFormatTypesResponse() throws JAXBException {
    return ResponseHelper.getInstance()
      .writeToString(buildBaseResponse(VerbType.LIST_METADATA_FORMATS).withListMetadataFormats(getMetadataFormatTypes()));
  }

  /**
   * Creates {@link OAIPMH} with Error id-does-not-exist
   * @return basic {@link OAIPMH}
   */
  private String buildIdentifierNotFound() throws JAXBException {
    return ResponseHelper.getInstance()
      .writeToString(buildBaseResponse(VerbType.LIST_METADATA_FORMATS)
        .withErrors(objectFactory.createOAIPMHerrorType()
          .withValue("Identifier not found")
          .withCode(OAIPMHerrorcodeType.ID_DOES_NOT_EXIST)));
  }

  /**
   * Creates ListMetadataFormatsType of supported metadata formats
   * @return supported metadata formats
   */
  private ListMetadataFormatsType getMetadataFormatTypes() {
    ListMetadataFormatsType mft = objectFactory.createListMetadataFormatsType();
    for (MetadataPrefix mp : MetadataPrefix.values()) {
      mft.withMetadataFormats(objectFactory.createMetadataFormatType()
        .withMetadataPrefix(mp.getName())
        .withSchema(mp.getSchema())
        .withMetadataNamespace(mp.getMetadataNamespace()));
    }
    return mft;
  }
}
