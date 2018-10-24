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
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.openarchives.oai._2.ListMetadataFormatsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.ObjectFactory;
import org.openarchives.oai._2.VerbType;

public class GetOaiMetadataFormatsHelper extends AbstractHelper {

  private static final Logger logger = Logger.getLogger(GetOaiMetadataFormatsHelper.class);

  private final Context ctx;
  private final HttpClientInterface httpClient;
  private final Map<String, String> okapiHeaders;

  private ObjectFactory objectFactory = new ObjectFactory();

  public GetOaiMetadataFormatsHelper(HttpClientInterface httpClient, Context ctx,
    Map<String, String> okapiHeaders) {
    this.httpClient = httpClient;
    this.okapiHeaders = okapiHeaders;
    this.ctx = ctx;
  }

  @Override
  public CompletableFuture<String> handle(Request request, Context ctx) {
    String identifier = request.getIdentifier();
    if(identifier!=null) {
      return retrieveMetadataFormats(identifier);
    } else {
      return retrieveMetadataFormats();
    }
  }

  /**
   * Processes request with identifier
   * @return future with {@link OAIPMH} response
   */
  private CompletableFuture<String> retrieveMetadataFormats(String identifier) {
    CompletableFuture<String> future = new VertxCompletableFuture<>(ctx);
    try {
      httpClient.request(HttpMethod.GET, "/instance-storage/instances/" + identifier, okapiHeaders)
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
  private CompletableFuture<String> retrieveMetadataFormats() {
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
   * Validates inventory-mod-storage response and returns {@link OAIPMH}
   * with populated MetadataFormatTypes or needed Errors according to OAI-PMH2 specification
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
   * @return basic {@link OAIPMH}
   */
  private OAIPMH buildMetadataFormatTypesResponse() {
    return objectFactory.createOAIPMH()
      .withListMetadataFormats(getMetadataFormatTypes())
      .withResponseDate(Instant.now().truncatedTo(ChronoUnit.SECONDS))
      .withRequest(objectFactory.createRequestType()
        .withVerb(VerbType.LIST_METADATA_FORMATS));
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
    return objectFactory.createListMetadataFormatsType()
      .withMetadataFormats(objectFactory.createMetadataFormatType()
        .withMetadataPrefix(MetadataPrefix.DC.getName())
        .withSchema(MetadataPrefix.DC.getSchema())
        .withMetadataNamespace(MetadataPrefix.DC.getMetadataNamespace()))
      .withMetadataFormats(objectFactory.createMetadataFormatType()
        .withMetadataPrefix(MetadataPrefix.MARC_XML.getName())
        .withSchema(MetadataPrefix.MARC_XML.getSchema())
        .withMetadataNamespace(MetadataPrefix.MARC_XML.getMetadataNamespace()));
  }
}
