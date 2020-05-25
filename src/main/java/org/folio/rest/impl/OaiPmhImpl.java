package org.folio.rest.impl;

import static io.vertx.core.Future.succeededFuture;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.REQUEST_PARAMS;
import static org.folio.oaipmh.Constants.FROM_PARAM;
import static org.folio.oaipmh.Constants.GENERIC_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.IDENTIFIER_PARAM;
import static org.folio.oaipmh.Constants.METADATA_PREFIX_PARAM;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_ENABLE_OAI_SERVICE;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_PARAM;
import static org.folio.oaipmh.Constants.SET_PARAM;
import static org.folio.oaipmh.Constants.SHOULD_LOAD_CONFIGS;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getProperty;
import static org.openarchives.oai._2.VerbType.GET_RECORD;
import static org.openarchives.oai._2.VerbType.IDENTIFY;
import static org.openarchives.oai._2.VerbType.LIST_IDENTIFIERS;
import static org.openarchives.oai._2.VerbType.LIST_METADATA_FORMATS;
import static org.openarchives.oai._2.VerbType.LIST_RECORDS;
import static org.openarchives.oai._2.VerbType.LIST_SETS;

import java.net.URLDecoder;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.domain.Verb;
import org.folio.oaipmh.helpers.GetOaiIdentifiersHelper;
import org.folio.oaipmh.helpers.GetOaiMetadataFormatsHelper;
import org.folio.oaipmh.helpers.GetOaiRecordHelper;
import org.folio.oaipmh.helpers.GetOaiRecordsHelper;
import org.folio.oaipmh.helpers.GetOaiRepositoryInfoHelper;
import org.folio.oaipmh.helpers.GetOaiSetsHelper;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.VerbHelper;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.validator.Validator;
import org.folio.oaipmh.validator.impl.VerbValidator;
import org.folio.rest.jaxrs.resource.Oai;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.VerbType;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;

public class OaiPmhImpl implements Oai {

  private final Logger logger = LoggerFactory.getLogger(OaiPmhImpl.class);

  private static final String UNKNOWN_VERB = "Unknown";

  /** Map containing OAI-PMH verb and corresponding helper instance. */
  private static final Map<VerbType, VerbHelper> HELPERS = new EnumMap<>(VerbType.class);

  private Validator validator = new VerbValidator();

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
  public void getOaiVerbs(String verbName, String identifier, String resumptionToken, String from, String until, String set, String metadataPrefix, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {

        Request request = Request.builder()
          .verb(VerbType.VERB)
          .okapiHeaders(okapiHeaders)
          .build();

        if (!getBooleanProperty(okapiHeaders, REPOSITORY_ENABLE_OAI_SERVICE)) {
          ResponseHelper responseHelper = ResponseHelper.getInstance();
          OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, OAIPMHerrorcodeType.SERVICE_UNAVAILABLE, "OAI-PMH service is disabled");
          asyncResultHandler.handle(Future.succeededFuture(responseHelper.buildFailureResponse(oaipmh, request)));
          return;
        }
        Map<String, String> contextParams = new HashMap<>();
        addParamToMapIfNotEmpty(RESUMPTION_TOKEN_PARAM, resumptionToken, contextParams);
        addParamToMapIfNotEmpty(FROM_PARAM, from, contextParams);
        addParamToMapIfNotEmpty(UNTIL_PARAM, until, contextParams);
        addParamToMapIfNotEmpty(SET_PARAM, set, contextParams);
        addParamToMapIfNotEmpty(METADATA_PREFIX_PARAM, metadataPrefix, contextParams);
        addParamToMapIfNotEmpty(IDENTIFIER_PARAM, identifier, contextParams);
        vertxContext.put(REQUEST_PARAMS, contextParams);

        List<OAIPMHerrorType> errors = validator.validate(verbName, vertxContext);

        if (isNotEmpty(errors)) {
          ResponseHelper responseHelper = ResponseHelper.getInstance();
          OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, errors);
          asyncResultHandler.handle(Future.succeededFuture(responseHelper.buildFailureResponse(oaipmh, request)));
        } else {
          vertxContext.put(SHOULD_LOAD_CONFIGS,false);
          continueRequestProcessing(Verb.fromName(verbName), okapiHeaders, asyncResultHandler, vertxContext);
        }
      }).exceptionally(handleError(asyncResultHandler, VerbType.fromValue(UNKNOWN_VERB)));
  }

  @Override
  public void getOaiRecords(String resumptionToken, String from, String until, String set, String metadataPrefix,
                            Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                            Context vertxContext) {
    RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, vertxContext)
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
            logger.debug("ListRecords response: {}", response.getEntity());
            asyncResultHandler.handle(succeededFuture(response));
          })
          .exceptionally(handleError(asyncResultHandler, LIST_RECORDS));
      }).exceptionally(handleError(asyncResultHandler, LIST_RECORDS));
  }

  @Override
  public void getOaiRecordsById(String id, String metadataPrefix, Map<String, String> okapiHeaders,
                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, vertxContext)
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
              logger.debug("GetRecord response: {}", response.getEntity());
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
    RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, vertxContext)
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
            logger.debug("ListIdentifiers response: {}", response.getEntity());
            asyncResultHandler.handle(succeededFuture(response));
          }).exceptionally(handleError(asyncResultHandler, LIST_IDENTIFIERS));
      }).exceptionally(handleError(asyncResultHandler, LIST_IDENTIFIERS));
  }

  @Override
  public void getOaiMetadataFormats(String identifier, Map<String, String> okapiHeaders,
                                    Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, vertxContext)
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
            logger.debug("ListMetadataFormats response: {}", response.getEntity());
            asyncResultHandler.handle(succeededFuture(response));
          }).exceptionally(handleError(asyncResultHandler, LIST_METADATA_FORMATS));
      }).exceptionally(handleError(asyncResultHandler, LIST_METADATA_FORMATS));
  }

  @Override
  public void getOaiSets(String resumptionToken, Map<String, String> okapiHeaders,
                         Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, vertxContext)
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
            logger.debug("ListSets response: {}", response.getEntity());
            asyncResultHandler.handle(succeededFuture(response));
          }).exceptionally(handleError(asyncResultHandler, LIST_SETS));
      }).exceptionally(handleError(asyncResultHandler, LIST_SETS));
  }

  @Override
  public void getOaiRepositoryInfo(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                   Context vertxContext) {
    RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {
        Request request = Request.builder()
          .baseURL(getProperty(okapiHeaders.get(OKAPI_TENANT), REPOSITORY_BASE_URL))
          .verb(IDENTIFY)
          .okapiHeaders(okapiHeaders)
          .build();

        HELPERS.get(IDENTIFY)
          .handle(request, vertxContext)
          .thenAccept(response -> {
            logger.debug("Identify response: {}", response.getEntity());
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

  private void addParamToMapIfNotEmpty(String paramName, String paramValue, Map<String, String> map) {
    if(StringUtils.isNotEmpty(paramValue)) {
      map.put(paramName, paramValue);
    }
  }

  private void continueRequestProcessing(Verb verb, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context context) {
    Map<String, String> requestParams = context.get(REQUEST_PARAMS);
    String resumptionToken = requestParams.get(RESUMPTION_TOKEN_PARAM);
    String from = requestParams.get(FROM_PARAM);
    String until = requestParams.get(UNTIL_PARAM);
    String set = requestParams.get(SET_PARAM);
    String metadataPrefix = requestParams.get(METADATA_PREFIX_PARAM);
    String identifier = requestParams.get(IDENTIFIER_PARAM);
    context.remove(REQUEST_PARAMS);

    switch (verb) {
      case LIST_RECORDS: getOaiRecords(resumptionToken, from, until, set, metadataPrefix, okapiHeaders, asyncResultHandler, context);
      break;
      case GET_RECORD: getOaiRecordsById(identifier, metadataPrefix, okapiHeaders, asyncResultHandler, context);
      break;
      case LIST_IDENTIFIERS: getOaiIdentifiers(resumptionToken, from, until, set, metadataPrefix, okapiHeaders, asyncResultHandler, context);
      break;
      case LIST_METADATA_FORMATS: getOaiMetadataFormats(identifier, okapiHeaders, asyncResultHandler, context);
      break;
      case LIST_SETS: getOaiSets(resumptionToken, okapiHeaders, asyncResultHandler, context);
      break;
      case IDENTIFY: getOaiRepositoryInfo(okapiHeaders, asyncResultHandler, context);
    }
  }

}
