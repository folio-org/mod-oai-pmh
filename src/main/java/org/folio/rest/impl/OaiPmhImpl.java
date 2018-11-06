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
import org.folio.oaipmh.helpers.RepositoryConfigurationHelper;
import org.folio.oaipmh.helpers.VerbHelper;
import org.folio.rest.jaxrs.resource.Oai;
import org.openarchives.oai._2.VerbType;

import javax.ws.rs.core.Response;
import java.net.URLDecoder;
import java.util.EnumMap;
import java.util.Map;

import static io.vertx.core.Future.succeededFuture;
import static org.folio.oaipmh.Constants.GENERIC_ERROR_MESSAGE;
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
    RepositoryConfigurationHelper.getInstance().getConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {

        Request request = Request.builder()
          .okapiHeaders(okapiHeaders)
          .from(from).metadataPrefix(metadataPrefix).resumptionToken(resumptionToken).set(set).until(until)
          .baseURL(getProperty(REPOSITORY_BASE_URL, vertxContext))
          .verb(LIST_RECORDS)
          .build();

         HELPERS.get(LIST_RECORDS)
           .handle(request, vertxContext)
           .thenAccept(response -> asyncResultHandler.handle(succeededFuture(response)))
           .exceptionally(throwable -> {
             asyncResultHandler.handle(succeededFuture(GetOaiRecordsResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE)));
             return null;
           });
      });
  }

  @Override
  public void getOaiRecordsById(String id, String metadataPrefix, Map<String, String> okapiHeaders,
                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    RepositoryConfigurationHelper.getInstance().getConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {
        try {

          Request request = Request.builder()
            .identifier(URLDecoder.decode(id, "UTF-8"))
            .okapiHeaders(okapiHeaders)
            .baseURL(getProperty(REPOSITORY_BASE_URL, vertxContext))
            .metadataPrefix(metadataPrefix)
            .verb(GET_RECORD)
            .build();

          HELPERS.get(GET_RECORD)
            .handle(request, vertxContext)
            .thenAccept(oai -> asyncResultHandler.handle(succeededFuture(oai)))
            .exceptionally(throwable -> {
              asyncResultHandler.handle(succeededFuture(GetOaiRecordsByIdResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE)));
              return null;
            });
        } catch (Exception e) {
          asyncResultHandler.handle(succeededFuture(GetOaiRecordsByIdResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE)));
        }
      });
  }

  @Override
  public void getOaiIdentifiers(String resumptionToken, String from, String until, String set, String metadataPrefix,
                                Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                Context vertxContext) {
    RepositoryConfigurationHelper.getInstance().getConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {

        Request request = Request.builder()
          .okapiHeaders(okapiHeaders)
          .baseURL(getProperty(REPOSITORY_BASE_URL, vertxContext))
          .from(from).metadataPrefix(metadataPrefix).resumptionToken(resumptionToken).set(set).until(until)
          .verb(LIST_IDENTIFIERS)
          .build();

        HELPERS.get(LIST_IDENTIFIERS)
               .handle(request, vertxContext)
               .thenAccept(oai -> asyncResultHandler.handle(succeededFuture(oai)))
               .exceptionally(throwable -> {
                 asyncResultHandler.handle(succeededFuture(GetOaiIdentifiersResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE)));
                 return null;
               });
      });
  }

  @Override
  public void getOaiMetadataFormats(String identifier, Map<String, String> okapiHeaders,
                                    Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    RepositoryConfigurationHelper.getInstance().getConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {
        Request request = Request.builder()
          .identifier(identifier)
          .baseURL(getProperty(REPOSITORY_BASE_URL, vertxContext))
          .okapiHeaders(okapiHeaders)
          .verb(LIST_METADATA_FORMATS)
          .build();
        request.getOaiRequest().withVerb(LIST_METADATA_FORMATS);
        VerbHelper getRepositoryInfoHelper = HELPERS.get(LIST_METADATA_FORMATS);
        getRepositoryInfoHelper.handle(request, vertxContext)
          .thenAccept(response -> {
            logger.debug("Successfully retrieved ListMetadataFormats info: " + response.getEntity().toString());
            asyncResultHandler.handle(succeededFuture(response));
          }).exceptionally(throwable -> {
            asyncResultHandler.handle(succeededFuture(GetOaiMetadataFormatsResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE)));
            return null;
        });
      });
  }

  @Override
  public void getOaiSets(String resumptionToken, Map<String, String> okapiHeaders,
                         Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    RepositoryConfigurationHelper.getInstance().getConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {

        Request request = Request.builder()
          .okapiHeaders(okapiHeaders)
          .baseURL(getProperty(REPOSITORY_BASE_URL, vertxContext))
          .resumptionToken(resumptionToken)
          .verb(LIST_SETS)
          .build();

        VerbHelper getSetsHelper = HELPERS.get(LIST_SETS);
        getSetsHelper.handle(request, vertxContext)
          .thenAccept(response -> {
            logger.info("Successfully retrieved sets structure: " + response.getEntity().toString());
            asyncResultHandler.handle(succeededFuture(response));
          }).exceptionally(throwable -> {
            asyncResultHandler.handle(succeededFuture(GetOaiSetsResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE)));
            return null;
        });
      });
  }

  @Override
  public void getOaiRepositoryInfo(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                   Context vertxContext) {
    RepositoryConfigurationHelper.getInstance().getConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {

        Request request = Request.builder()
          .okapiHeaders(okapiHeaders)
          .baseURL(getProperty(REPOSITORY_BASE_URL, vertxContext))
          .baseURL(vertxContext.config().getString(REPOSITORY_BASE_URL))
          .verb(IDENTIFY).build();

        VerbHelper getRepositoryInfoHelper = HELPERS.get(IDENTIFY);
        getRepositoryInfoHelper.handle(request, vertxContext)
          .thenAccept(response -> {
            logger.info("Successfully retrieved repository info: " + response.getEntity().toString());
            asyncResultHandler.handle(succeededFuture(response));
          }).exceptionally(throwable -> {
            asyncResultHandler.handle(succeededFuture(GetOaiRepositoryInfoResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE)));
            return null;
          });
      });
  }


}
