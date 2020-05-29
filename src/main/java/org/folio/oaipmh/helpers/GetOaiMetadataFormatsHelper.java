package org.folio.oaipmh.helpers;

import static org.folio.oaipmh.Constants.INVALID_IDENTIFIER_ERROR_MESSAGE;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.folio.oaipmh.Constants;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.rest.tools.client.Response;
import org.openarchives.oai._2.ListMetadataFormatsType;
import org.openarchives.oai._2.MetadataFormatType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorcodeType;

import io.vertx.core.Context;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;


public class GetOaiMetadataFormatsHelper extends AbstractHelper {

  private static final Logger logger = LoggerFactory.getLogger(GetOaiMetadataFormatsHelper.class);

  @Override
  public CompletableFuture<javax.ws.rs.core.Response> handle(Request request, Context ctx) {
    return retrieveMetadataFormats(request, ctx);
  }

  /**
   * Processes request with identifier
   * @return future with {@link OAIPMH} response
   */
  private CompletableFuture<javax.ws.rs.core.Response> retrieveMetadataFormats(Request request, Context ctx) {
    if (request.getIdentifier() == null) {
      return VertxCompletableFuture.completedFuture(retrieveMetadataFormats(request));
    } else if (!validateIdentifier(request)) {
      return VertxCompletableFuture.completedFuture(buildBadArgumentResponse(request));
    }

    CompletableFuture<javax.ws.rs.core.Response> future = new VertxCompletableFuture<>(ctx);
    Map<String, String> okapiHeaders = request.getOkapiHeaders();
    try {
      String endpoint = storageHelper.buildRecordsEndpoint(request, false);
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
    OAIPMH oaipmh = getResponseHelper().buildBaseOaipmhResponse(request).withListMetadataFormats(getMetadataFormatTypes());
    return getResponseHelper().buildSuccessResponse(oaipmh);
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
      // The storage service could not return an instance so we have to send 500 back to client
      logger.error("No instance found. Service responded with error: " + response.getError());
      throw new IllegalStateException(response.getError().toString());
    }
    return buildIdentifierNotFoundResponse(request);
  }

  /**
   * Builds {@linkplain javax.ws.rs.core.Response Response} with 'badArgument' error because passed identifier is invalid.
   * @return {@linkplain javax.ws.rs.core.Response Response}  with {@link OAIPMH} response
   */
  private javax.ws.rs.core.Response buildBadArgumentResponse(Request request) {
    ResponseHelper responseHelper = getResponseHelper();
    OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, OAIPMHerrorcodeType.BAD_ARGUMENT, INVALID_IDENTIFIER_ERROR_MESSAGE);
    return responseHelper.buildFailureResponse(oaipmh, request);
  }

  /**
   * Builds {@linkplain javax.ws.rs.core.Response Response} with 'id-does-not-exist' error because passed identifier isn't exist.
   * @return {@linkplain javax.ws.rs.core.Response Response}  with {@link OAIPMH} response
   */
  private javax.ws.rs.core.Response buildIdentifierNotFoundResponse(Request request) {
    OAIPMH oaipmh = getResponseHelper().buildOaipmhResponseWithErrors(request, OAIPMHerrorcodeType.ID_DOES_NOT_EXIST, Constants.RECORD_NOT_FOUND_ERROR);
    return getResponseHelper().buildFailureResponse(oaipmh, request);
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
