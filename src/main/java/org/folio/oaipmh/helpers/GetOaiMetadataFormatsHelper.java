package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
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
    } else if (!validateIdentifier(request, ctx)) {
      return VertxCompletableFuture.completedFuture(buildBadArgumentResponse(request));
    }

    CompletableFuture<javax.ws.rs.core.Response> future = new VertxCompletableFuture<>(ctx);
    Map<String, String> okapiHeaders = request.getOkapiHeaders();
    try {
      String endpoint = storageHelper.getInstanceEndpoint(extractStorageIdentifier(request, ctx));
      getOkapiClient(okapiHeaders)
        .request(HttpMethod.GET, endpoint, okapiHeaders)
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
   * @return {@linkplain javax.ws.rs.core.Response Response} with Identifier not found error
   */
  private javax.ws.rs.core.Response verifyAndGetOaiPmhResponse(Request request, Response response) {
    if (Response.isSuccess(response.getCode())) {
      JsonArray instances = storageHelper.getItems(response.getBody());
      if (instances != null && !instances.isEmpty()) {
        return retrieveMetadataFormats(request);
      }
    } else {
      logger.error("No instances found. Service responded with error: " + response.getError());
    }
    return GetOaiMetadataFormatsResponse.respond404WithApplicationXml(buildIdentifierNotFound(request));
  }

  /**
   * Builds {@linkplain javax.ws.rs.core.Response Response} with 'badArgument' error because passed identifier is invalid
   * @return {@linkplain javax.ws.rs.core.Response Response}  with {@link OAIPMH} response
   */
  private javax.ws.rs.core.Response buildBadArgumentResponse(Request request) {
    return GetOaiMetadataFormatsResponse.respond422WithApplicationXml(buildOaipmhWithBadArgumentError(request));
  }

  /**
   * Creates {@link OAIPMH} with ListMetadataFormats element
   *
   * @param request {@link Request}
   * @return basic {@link OAIPMH}
   */
  private String buildMetadataFormatTypesResponse(Request request) {
      return convertToString(buildBaseResponse(request.getOaiRequest())
                             .withListMetadataFormats(getMetadataFormatTypes()));
  }

  /**
   * Creates {@link OAIPMH} with Error id-does-not-exist
   * @param request {@link Request}
   * @return basic {@link OAIPMH}
   */
  private String buildIdentifierNotFound(Request request) {
    return convertToString(buildBaseResponse(request.getOaiRequest())
        .withErrors(new OAIPMHerrorType()
          .withValue(String.format("%s has the structure of a valid identifier, but it maps to no known item", request.getIdentifier()))
          .withCode(OAIPMHerrorcodeType.ID_DOES_NOT_EXIST)));
  }

  /**
   * Creates {@link OAIPMH} with Error id-does-not-exist
   * @param request {@link Request}
   * @return basic {@link OAIPMH}
   */
  private String buildOaipmhWithBadArgumentError(Request request) {
    return convertToString(buildBaseResponse(request.getOaiRequest())
      .withErrors(new OAIPMHerrorType()
        .withCode(OAIPMHerrorcodeType.BAD_ARGUMENT)
        .withValue(String.format("%s has the structure of an invalid identifier", request.getIdentifier()))));
  }

  /**
   * Marshals {@link OAIPMH} to string. In case the {@link OAIPMH} is invalid, the {@link IllegalStateException} is thrown
   * @param oaipmhResponse {@link OAIPMH} to marshal
   * @return string representation of the {@link OAIPMH}
   */
  private String convertToString(OAIPMH oaipmhResponse) {
    try {
      return ResponseHelper.getInstance().writeToString(oaipmhResponse);
    } catch(JAXBException e) {
      logger.error("Error marshalling response: " + e.getMessage());
      throw new IllegalStateException(e);
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
