package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
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
import org.folio.oaipmh.helpers.RepositoryConfigurationHelper;
import org.folio.oaipmh.helpers.VerbHelper;
import org.folio.rest.jaxrs.resource.Oai;
import org.openarchives.oai._2.VerbType;

import javax.ws.rs.core.Response;
import java.net.URLDecoder;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;

import static io.vertx.core.Future.succeededFuture;
import static org.folio.oaipmh.Constants.GENERIC_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.helpers.RepositoryConfigurationHelper.getProperty;
import static org.openarchives.oai._2.VerbType.GET_RECORD;
import static org.openarchives.oai._2.VerbType.IDENTIFY;
import static org.openarchives.oai._2.VerbType.LIST_IDENTIFIERS;
import static org.openarchives.oai._2.VerbType.LIST_METADATA_FORMATS;
import static org.openarchives.oai._2.VerbType.LIST_RECORDS;
import static org.openarchives.oai._2.VerbType.LIST_SETS;

public class OaiPmhImpl implements Oai {
  private final Logger logger = LoggerFactory.getLogger(OaiPmhImpl.class);

  /** Map containing OAI-PMH verb and corresponding helper instance. */
  private static final Map<VerbType, VerbHelper> HELPERS = new EnumMap<>(VerbType.class);

  public static void init(Handler<AsyncResult<Boolean>> resultHandler) {
    HELPERS.put(IDENTIFY, new GetOaiRepositoryInfoHelper());
    HELPERS.put(LIST_IDENTIFIERS, new GetOaiIdentifiersHelper());
    HELPERS.put(LIST_RECORDS, new GetOaiRecordsHelper());
    HELPERS.put(LIST_SETS, new GetOaiSetsHelper());
    HELPERS.put(LIST_METADATA_FORMATS, new GetOaiMetadataFormatsHelper());
    HELPERS.put(GET_RECORD, new GetOaiRecordHelper());

    resultHandler.handle(succeededFuture(true));
  }

  @Override
  public void getOaiRecords(String resumptionToken, String from, String until, String set, String metadataPrefix,
                            Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                            Context vertxContext) {
    new RepositoryConfigurationHelper().getConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {

        Request request = Request.builder()
                                  .okapiHeaders(okapiHeaders)
                                  .baseURL(getProperty(okapiHeaders.get(OKAPI_TENANT), REPOSITORY_BASE_URL))
                                  .verb(LIST_RECORDS)
                                  .from(from).metadataPrefix(metadataPrefix).resumptionToken(resumptionToken).set(set).until(until)
                                  .build();

        HELPERS.get(LIST_RECORDS)
          .handle(request, vertxContext)
          .thenAccept(response -> {
            if (logger.isDebugEnabled()) {
              logger.debug("ListRecords response: " + response.getEntity());
            }
            asyncResultHandler.handle(succeededFuture(response));
          })
          .exceptionally(handleError(asyncResultHandler, LIST_RECORDS));
      }).exceptionally(handleError(asyncResultHandler, LIST_RECORDS));
  }

  @Override
  public void getOaiRecordsById(String id, String metadataPrefix, Map<String, String> okapiHeaders,
                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    new RepositoryConfigurationHelper().getConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {
        try {
          Request request = Request.builder()
            .identifier(URLDecoder.decode(id, "UTF-8"))
            .okapiHeaders(okapiHeaders)
            .verb(GET_RECORD)
            .baseURL(getProperty(okapiHeaders.get(OKAPI_TENANT), REPOSITORY_BASE_URL))
            .metadataPrefix(metadataPrefix)
            .build();

          HELPERS.get(GET_RECORD)
            .handle(request, vertxContext)
            .thenAccept(response -> {
              if (logger.isDebugEnabled()) {
                logger.debug("GetRecord response: " + response.getEntity());
              }
              asyncResultHandler.handle(succeededFuture(response));
            })
            .exceptionally(handleError(asyncResultHandler, GET_RECORD));
        } catch (Exception e) {
          asyncResultHandler.handle(getFutureWithErrorResponse(GET_RECORD));
        }
      }).exceptionally(handleError(asyncResultHandler, GET_RECORD));
  }

  @Override
  public void getOaiIdentifiers(String resumptionToken, String from, String until, String set, String metadataPrefix,
                                Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                Context vertxContext) {
    new RepositoryConfigurationHelper().getConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {

        Request request = Request.builder()
                                  .okapiHeaders(okapiHeaders)
                                  .baseURL(getProperty(okapiHeaders.get(OKAPI_TENANT), REPOSITORY_BASE_URL))
                                  .verb(LIST_IDENTIFIERS)
                                  .from(from).metadataPrefix(metadataPrefix).resumptionToken(resumptionToken).set(set).until(until)
                                  .build();

        HELPERS.get(LIST_IDENTIFIERS)
          .handle(request, vertxContext)
          .thenAccept(response -> {
            if (logger.isDebugEnabled()) {
              logger.debug("ListIdentifiers response: " + response.getEntity());
            }
            asyncResultHandler.handle(succeededFuture(response));
          }).exceptionally(handleError(asyncResultHandler, LIST_IDENTIFIERS));
      }).exceptionally(handleError(asyncResultHandler, LIST_IDENTIFIERS));
  }

  @Override
  public void getOaiMetadataFormats(String identifier, Map<String, String> okapiHeaders,
                                    Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    new RepositoryConfigurationHelper().getConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {
        Request request = Request.builder()
                                  .identifier(identifier)
                                  .verb(LIST_METADATA_FORMATS)
                                  .baseURL(getProperty(okapiHeaders.get(OKAPI_TENANT), REPOSITORY_BASE_URL))
                                  .okapiHeaders(okapiHeaders)
                                  .build();
        HELPERS.get(LIST_METADATA_FORMATS)
          .handle(request, vertxContext)
          .thenAccept(response -> {
            if (logger.isDebugEnabled()) {
              logger.debug("ListMetadataFormats response: " + response.getEntity());
            }
            asyncResultHandler.handle(succeededFuture(response));
          }).exceptionally(handleError(asyncResultHandler, LIST_METADATA_FORMATS));
      }).exceptionally(handleError(asyncResultHandler, LIST_METADATA_FORMATS));
  }

  @Override
  public void getOaiSets(String resumptionToken, Map<String, String> okapiHeaders,
                         Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    new RepositoryConfigurationHelper().getConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {

        Request request = Request.builder()
          .okapiHeaders(okapiHeaders)
          .verb(LIST_SETS)
          .baseURL(getProperty(okapiHeaders.get(OKAPI_TENANT), REPOSITORY_BASE_URL))
          .resumptionToken(resumptionToken)
          .build();

        HELPERS.get(LIST_SETS)
          .handle(request, vertxContext)
          .thenAccept(response -> {
            if (logger.isDebugEnabled()) {
              logger.debug("ListSets response: " + response.getEntity());
            }
            asyncResultHandler.handle(succeededFuture(response));
          }).exceptionally(handleError(asyncResultHandler, LIST_SETS));
      }).exceptionally(handleError(asyncResultHandler, LIST_SETS));
  }

  @Override
  public void getOaiRepositoryInfo(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                   Context vertxContext) {
    new RepositoryConfigurationHelper().getConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {
        Request request = Request.builder()
          .baseURL(getProperty(okapiHeaders.get(OKAPI_TENANT), REPOSITORY_BASE_URL))
          .verb(IDENTIFY)
          .okapiHeaders(okapiHeaders)
          .build();

        HELPERS.get(IDENTIFY)
          .handle(request, vertxContext)
          .thenAccept(response -> {
            if (logger.isDebugEnabled()) {
              logger.debug("Identify response: " + response.getEntity());
            }
            asyncResultHandler.handle(succeededFuture(response));
          }).exceptionally(handleError(asyncResultHandler, IDENTIFY));
      }).exceptionally(handleError(asyncResultHandler, IDENTIFY));
  }

  private Function<Throwable, Void> handleError(Handler<AsyncResult<Response>>
                                                               asyncResultHandler, VerbType verb) {
    return throwable -> {
      asyncResultHandler.handle(getFutureWithErrorResponse(verb));
      return null;
    };
  }

  private Future<Response> getFutureWithErrorResponse(VerbType verb) {
    Response errorResponse;
    switch (verb) {
      case GET_RECORD: errorResponse = GetOaiRecordsByIdResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE);
        break;
      case LIST_RECORDS: errorResponse = GetOaiRecordsResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE);
        break;
      case LIST_IDENTIFIERS: errorResponse = GetOaiIdentifiersResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE);
        break;
      case LIST_METADATA_FORMATS: errorResponse = GetOaiMetadataFormatsResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE);
        break;
      case LIST_SETS: errorResponse = GetOaiSetsResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE);
        break;
      case IDENTIFY: errorResponse = GetOaiRepositoryInfoResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE);
        break;
      default: errorResponse = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
    return succeededFuture(errorResponse);
  }
}
