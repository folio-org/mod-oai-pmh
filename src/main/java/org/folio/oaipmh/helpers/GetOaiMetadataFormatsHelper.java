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
import org.folio.rest.tools.client.Response;
import org.openarchives.oai._2.ListMetadataFormatsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.VerbType;

public class GetOaiMetadataFormatsHelper extends AbstractHelper {

  private static final Logger logger = Logger.getLogger(GetOaiMetadataFormatsHelper.class);

  @Override
  public CompletableFuture<String> handle(Request request, Context ctx) {
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
  private CompletableFuture<String> retrieveMetadataFormats(String identifier, Context ctx, Map<String, String> okapiHeaders) {
    CompletableFuture<String> future = new VertxCompletableFuture<>(ctx);
    try {
      getHttpClient(okapiHeaders).request(HttpMethod.GET, "/instance-storage/instances/" + identifier, okapiHeaders)
        .thenApply(this::verifyAndGetOaiPmhResponse)
        .thenAccept(oaipmh -> {
          try {
            String reply = ResponseHelper.getInstance().writeToString(oaipmh);
            future.complete(reply);
          } catch (JAXBException e) {
            logger.error("Error happened in OAI-PMH response marshalling", e);
          }
        })
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
  private CompletableFuture<String> retrieveMetadataFormats(Context ctx) {
    CompletableFuture<String> future = new VertxCompletableFuture<>(ctx);
    try {
      future
        .complete(ResponseHelper.getInstance().writeToString(buildMetadataFormatTypesResponse()));
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
  private OAIPMH verifyAndGetOaiPmhResponse(Response response) {
    if (!Response.isSuccess(response.getCode())) {
      return buildIdentifierNotFound();
    }
    return buildMetadataFormatTypesResponse();
  }

  /**
   * Creates {@link OAIPMH} with ListMetadataFormats element
   *
   * @return basic {@link OAIPMH}
   */
  private OAIPMH buildMetadataFormatTypesResponse() {
    return buildBaseResponse(VerbType.LIST_METADATA_FORMATS).withListMetadataFormats(getMetadataFormatTypes());
  }

  /**
   * Creates {@link OAIPMH} with Error id-does-not-exist
   * @return basic {@link OAIPMH}
   */
  private OAIPMH buildIdentifierNotFound() {
    return objectFactory.createOAIPMH()
      .withErrors(objectFactory.createOAIPMHerrorType().withValue("404")
        .withCode(OAIPMHerrorcodeType.ID_DOES_NOT_EXIST))
      .withResponseDate(Instant.now().truncatedTo(ChronoUnit.SECONDS))
      .withRequest(objectFactory.createRequestType()
        .withVerb(VerbType.LIST_METADATA_FORMATS));
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
