package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseHelper;
import org.folio.oaipmh.helpers.GetOaiIdentifiersHelper;
import org.folio.oaipmh.helpers.GetOaiMetadataFormatsHelper;
import org.folio.oaipmh.helpers.GetOaiRepositoryInfoHelper;
import org.folio.oaipmh.helpers.GetOaiSetsHelper;
import org.folio.oaipmh.helpers.VerbHelper;
import org.folio.rest.jaxrs.resource.Oai;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.ObjectFactory;
import org.openarchives.oai._2.VerbType;

import javax.ws.rs.core.Response;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.EnumMap;
import java.util.Map;

import static io.vertx.core.Future.succeededFuture;
import static org.openarchives.oai._2.VerbType.IDENTIFY;
import static org.openarchives.oai._2.VerbType.LIST_IDENTIFIERS;
import static org.openarchives.oai._2.VerbType.LIST_METADATA_FORMATS;
import static org.openarchives.oai._2.VerbType.LIST_SETS;

public class OaiPmhImpl implements Oai {
  private final Logger logger = LoggerFactory.getLogger(OaiPmhImpl.class);

  private static final String REPOSITORY_BASE_URL = "repository.baseURL";
  private static final String ERROR_MESSAGE = "Sorry, we can't process your request. Please contact administrator(s).";

  private ObjectFactory objectFactory = new ObjectFactory();

  /** Map containing OAI-PMH verb and corresponding helper instance. */
  private static final Map<VerbType, VerbHelper> HELPERS = new EnumMap<>(VerbType.class);

  public static void init(Handler<AsyncResult<Boolean>> resultHandler) {
    HELPERS.put(IDENTIFY, new GetOaiRepositoryInfoHelper());
    HELPERS.put(LIST_IDENTIFIERS, new GetOaiIdentifiersHelper());
    HELPERS.put(LIST_SETS, new GetOaiSetsHelper());
    HELPERS.put(LIST_METADATA_FORMATS, new GetOaiMetadataFormatsHelper());
    // other verb implementations to be added here

    resultHandler.handle(Future.succeededFuture(true));
  }

  @Override
  public void getOaiRecords(String resumptionToken, String from, String until, String set, String metadataPrefix,
                            Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                            Context vertxContext) {
    OAIPMH oai = buildBaseResponse(VerbType.LIST_RECORDS)
      .withErrors(objectFactory.createOAIPMHerrorType().withCode(OAIPMHerrorcodeType.NO_RECORDS_MATCH));
    oai.getRequest()
       .withResumptionToken(resumptionToken)
       //.withFrom(from != null ? Instant.parse(from) : null)
       //.withUntil(until != null ? Instant.parse(until) : null)
       .withSet(set)
       .withMetadataPrefix(metadataPrefix);

    try {
      String response = ResponseHelper.getInstance().writeToString(oai);
      asyncResultHandler.handle(succeededFuture(GetOaiRecordsResponse.respond422WithApplicationXml(response)));
    } catch (Exception e) {
      logger.error("Unexpected error happened while processing ListRecords verb request", e);
      asyncResultHandler.handle(succeededFuture(GetOaiRecordsResponse.respond500WithTextPlain(ERROR_MESSAGE)));
    }
  }

  @Override
  public void getOaiRecordsById(String id, String metadataPrefix, Map<String, String> okapiHeaders,
                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    OAIPMH oai = buildBaseResponse(VerbType.GET_RECORD)
      .withErrors(objectFactory.createOAIPMHerrorType().withCode(OAIPMHerrorcodeType.ID_DOES_NOT_EXIST));
    oai.getRequest().withIdentifier(id).withMetadataPrefix(metadataPrefix);

    try {
      String response = ResponseHelper.getInstance().writeToString(oai);
      asyncResultHandler.handle(succeededFuture(GetOaiRecordsByIdResponse.respond422WithApplicationXml(response)));
    } catch (Exception e) {
      logger.error("Unexpected error happened while processing GetRecord verb request", e);
      asyncResultHandler.handle(succeededFuture(GetOaiRecordsByIdResponse.respond500WithTextPlain(ERROR_MESSAGE)));
    }
  }

  @Override
  public void getOaiIdentifiers(String resumptionToken, String from, String until, String set, String metadataPrefix,
                                Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                Context vertxContext) {
    Request request = Request.builder()
                             .okapiHeaders(okapiHeaders)
                             .from(from).metadataPrefix(metadataPrefix).resumptionToken(resumptionToken).set(set).until(until)
                             .build();

    HELPERS.get(LIST_IDENTIFIERS)
           .handle(request, vertxContext)
           .thenAccept(oai -> {
             asyncResultHandler.handle(succeededFuture(oai));
           })
           .exceptionally(throwable -> {
             asyncResultHandler.handle(succeededFuture(GetOaiIdentifiersResponse.respond500WithTextPlain(ERROR_MESSAGE)));
             return null;
           });
  }

  @Override
  public void getOaiMetadataFormats(String identifier, Map<String, String> okapiHeaders,
                                    Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    Request request = Request.builder().identifier(identifier).okapiHeaders(okapiHeaders).build();
    VerbHelper getRepositoryInfoHelper = HELPERS.get(LIST_METADATA_FORMATS);
    getRepositoryInfoHelper.handle(request, vertxContext)
      .thenAccept(response -> {
        logger.info("Successfully retrieved ListMetadataFormats info: " + response.getEntity().toString());
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

    Request request = Request.builder().okapiHeaders(okapiHeaders).build();

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
