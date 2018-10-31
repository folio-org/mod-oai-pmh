package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.GetOaiIdentifiersHelper;
import org.folio.oaipmh.helpers.GetOaiMetadataFormatsHelper;
import org.folio.oaipmh.helpers.GetOaiRecordHelper;
import org.folio.oaipmh.helpers.GetOaiRecordsHelper;
import org.folio.oaipmh.helpers.GetOaiRepositoryInfoHelper;
import org.folio.oaipmh.helpers.GetOaiSetsHelper;
import org.folio.oaipmh.helpers.VerbHelper;
import org.folio.rest.jaxrs.resource.Oai;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.ObjectFactory;
import org.openarchives.oai._2.VerbType;

import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.EnumMap;
import java.util.Map;

import static io.vertx.core.Future.succeededFuture;
import static org.folio.oaipmh.Constants.IDENTIFIER_PREFIX;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.openarchives.oai._2.VerbType.*;

public class OaiPmhImpl implements Oai {
  private final Logger logger = LoggerFactory.getLogger(OaiPmhImpl.class);

  private static final String ERROR_MESSAGE = "Sorry, we can't process your request. Please contact administrator(s).";

  private ObjectFactory objectFactory = new ObjectFactory();

  /** Map containing OAI-PMH verb and corresponding helper instance. */
  private static final Map<VerbType, VerbHelper> HELPERS = new EnumMap<>(VerbType.class);

  public static void init(Handler<AsyncResult<Boolean>> resultHandler) {
    HELPERS.put(IDENTIFY, new GetOaiRepositoryInfoHelper());
    HELPERS.put(LIST_IDENTIFIERS, new GetOaiIdentifiersHelper());
    HELPERS.put(LIST_RECORDS, new GetOaiRecordsHelper());
    HELPERS.put(LIST_SETS, new GetOaiSetsHelper());
    HELPERS.put(LIST_METADATA_FORMATS, new GetOaiMetadataFormatsHelper());
    HELPERS.put(GET_RECORD, new GetOaiRecordHelper());
    // other verb implementations to be added here

    resultHandler.handle(succeededFuture(true));
  }

  @Override
  public void getOaiRecords(String resumptionToken, String from, String until, String set, String metadataPrefix,
                            Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                            Context vertxContext) {

    Request request = Request.builder()
                             .okapiHeaders(okapiHeaders)
                              .identifierPrefix(vertxContext.config().getString(IDENTIFIER_PREFIX))
                             .from(from).metadataPrefix(metadataPrefix).resumptionToken(resumptionToken).set(set).until(until)
                             .build();

    HELPERS.get(LIST_RECORDS)
           .handle(request, vertxContext)
           .thenAccept(response -> asyncResultHandler.handle(succeededFuture(response)))
           .exceptionally(throwable -> {
             asyncResultHandler.handle(succeededFuture(GetOaiRecordsResponse.respond500WithTextPlain(ERROR_MESSAGE)));
             return null;
           });
  }

  @Override
  public void getOaiRecordsById(String id, String metadataPrefix, Map<String, String> okapiHeaders,
                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    Request request = null;
    try {
      request = Request.builder()
        .identifier(URLDecoder.decode(id, "UTF-8"))
        .okapiHeaders(okapiHeaders)
        .metadataPrefix(metadataPrefix)
        .identifierPrefix(vertxContext.config().getString(IDENTIFIER_PREFIX))
        .build();
    } catch (UnsupportedEncodingException e) {
      asyncResultHandler.handle(succeededFuture(GetOaiIdentifiersResponse.respond500WithTextPlain(ERROR_MESSAGE)));
    }

    HELPERS.get(GET_RECORD)
      .handle(request, vertxContext)
      .thenAccept(oai -> asyncResultHandler.handle(succeededFuture(oai)))
      .exceptionally(throwable -> {
        asyncResultHandler.handle(succeededFuture(GetOaiIdentifiersResponse.respond500WithTextPlain(ERROR_MESSAGE)));
        return null;
      });
  }

  @Override
  public void getOaiIdentifiers(String resumptionToken, String from, String until, String set, String metadataPrefix,
                                Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                Context vertxContext) {
    Request request = Request.builder()
                             .okapiHeaders(okapiHeaders)
                              .identifierPrefix(vertxContext.config().getString(IDENTIFIER_PREFIX))
                             .from(from).metadataPrefix(metadataPrefix).resumptionToken(resumptionToken).set(set).until(until)
                             .build();

    HELPERS.get(LIST_IDENTIFIERS)
           .handle(request, vertxContext)
           .thenAccept(oai -> asyncResultHandler.handle(succeededFuture(oai)))
           .exceptionally(throwable -> {
             asyncResultHandler.handle(succeededFuture(GetOaiIdentifiersResponse.respond500WithTextPlain(ERROR_MESSAGE)));
             return null;
           });
  }

  @Override
  public void getOaiMetadataFormats(String identifier, Map<String, String> okapiHeaders,
                                    Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    Request request = Request.builder().identifier(identifier)
      .identifierPrefix(vertxContext.config().getString(IDENTIFIER_PREFIX))
      .okapiHeaders(okapiHeaders).build();
    VerbHelper getRepositoryInfoHelper = HELPERS.get(LIST_METADATA_FORMATS);
    getRepositoryInfoHelper.handle(request, vertxContext)
      .thenAccept(response -> {
        logger.debug("Successfully retrieved ListMetadataFormats info: " + response.getEntity().toString());
        asyncResultHandler.handle(succeededFuture(response));
      }).exceptionally(throwable -> {
        asyncResultHandler.handle(succeededFuture(GetOaiMetadataFormatsResponse.respond500WithTextPlain(ERROR_MESSAGE)));
        return null;
    });
  }

  @Override
  public void getOaiSets(String resumptionToken, Map<String, String> okapiHeaders,
                         Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    Request request = Request.builder()
      .okapiHeaders(okapiHeaders)
      .identifierPrefix(vertxContext.config().getString(IDENTIFIER_PREFIX))
      .resumptionToken(resumptionToken)
      .build();

    VerbHelper getSetsHelper = HELPERS.get(LIST_SETS);
    getSetsHelper.handle(request, vertxContext)
      .thenAccept(response -> {
        logger.info("Successfully retrieved sets structure: " + response.getEntity().toString());
        asyncResultHandler.handle(succeededFuture(response));
      }).exceptionally(throwable -> {
        asyncResultHandler.handle(succeededFuture(GetOaiSetsResponse.respond500WithTextPlain(ERROR_MESSAGE)));
        return null;
    });
  }

  @Override
  public void getOaiRepositoryInfo(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                   Context vertxContext) {

    Request request = Request.builder()
      .identifierPrefix(vertxContext.config().getString(IDENTIFIER_PREFIX))
      .okapiHeaders(okapiHeaders).build();

    VerbHelper getRepositoryInfoHelper = HELPERS.get(IDENTIFY);
    getRepositoryInfoHelper.handle(request, vertxContext)
      .thenAccept(response -> {
          logger.info("Successfully retrieved repository info: " + response.getEntity().toString());
          asyncResultHandler.handle(succeededFuture(response));
      }).exceptionally(throwable -> {
        asyncResultHandler.handle(succeededFuture(GetOaiRepositoryInfoResponse.respond500WithTextPlain(ERROR_MESSAGE)));
        return null;
      });
  }

  /**
   * Creates basic {@link OAIPMH} with ResponseDate and Request details
   * @param verb {@link VerbType}
   * @return basic {@link OAIPMH}
   */
  private OAIPMH buildBaseResponse(VerbType verb) {
    return objectFactory.createOAIPMH()
      // According to spec the nanoseconds should not be used so truncate to seconds
      .withResponseDate(Instant.now().truncatedTo(ChronoUnit.SECONDS))
      .withRequest(objectFactory.createRequestType()
        .withVerb(verb)
        .withValue(System.getProperty(REPOSITORY_BASE_URL)));
  }
}
