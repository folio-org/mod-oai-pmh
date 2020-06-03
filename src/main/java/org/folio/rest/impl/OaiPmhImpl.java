package org.folio.rest.impl;

import static io.vertx.core.Future.succeededFuture;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.FROM_PARAM;
import static org.folio.oaipmh.Constants.GENERIC_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.IDENTIFIER_PARAM;
import static org.folio.oaipmh.Constants.METADATA_PREFIX_PARAM;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_ENABLE_OAI_SERVICE;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_PARAM;
import static org.folio.oaipmh.Constants.SET_PARAM;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getProperty;
import static org.openarchives.oai._2.VerbType.GET_RECORD;
import static org.openarchives.oai._2.VerbType.IDENTIFY;
import static org.openarchives.oai._2.VerbType.LIST_IDENTIFIERS;
import static org.openarchives.oai._2.VerbType.LIST_METADATA_FORMATS;
import static org.openarchives.oai._2.VerbType.LIST_RECORDS;
import static org.openarchives.oai._2.VerbType.LIST_SETS;

import io.vertx.ext.web.RoutingContext;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.GetOaiIdentifiersHelper;
import org.folio.oaipmh.helpers.GetOaiMetadataFormatsHelper;
import org.folio.oaipmh.helpers.GetOaiRecordHelper;
import org.folio.oaipmh.helpers.GetOaiRecordsHelper;
import org.folio.oaipmh.helpers.GetOaiRepositoryInfoHelper;
import org.folio.oaipmh.helpers.GetOaiSetsHelper;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.VerbHelper;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.processors.MarcWithHoldingsRequestHelper;
import org.folio.oaipmh.validator.VerbValidator;
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

public class OaiPmhImpl implements Oai {

  private final Logger logger = LoggerFactory.getLogger(OaiPmhImpl.class);

  /** Map containing OAI-PMH verb and corresponding helper instance. */
  private static final Map<VerbType, VerbHelper> HELPERS = new EnumMap<>(VerbType.class);

  private VerbValidator validator = new VerbValidator();

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
  public void getOaiRecords(String verb, String identifier, String resumptionToken,
                            String from, String until, String set, String metadataPrefix,
                            RoutingContext routingContext, Map<String, String> okapiHeaders,
                            Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, vertxContext)
      .thenAccept(v -> {
        try {
          Request.Builder requestBuilder = Request.builder()
            .okapiHeaders(okapiHeaders)
            .verb(getVerb(verb))
            .baseURL(getProperty(okapiHeaders.get(OKAPI_TENANT), REPOSITORY_BASE_URL))
            .from(from).metadataPrefix(metadataPrefix).resumptionToken(resumptionToken).set(set).until(until);
          if (StringUtils.isNotEmpty(identifier)) {
            requestBuilder.identifier(URLDecoder.decode(identifier, "UTF-8"));
          }

          Request request = requestBuilder.build();

          if (!getBooleanProperty(okapiHeaders, REPOSITORY_ENABLE_OAI_SERVICE)) {
            ResponseHelper responseHelper = ResponseHelper.getInstance();
            OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, OAIPMHerrorcodeType.SERVICE_UNAVAILABLE, "OAI-PMH service is disabled");
            asyncResultHandler.handle(Future.succeededFuture(responseHelper.buildFailureResponse(oaipmh, request)));
            return;
          }

          Map<String, String> requestParams = new HashMap<>();
          addParamToMapIfNotEmpty(IDENTIFIER_PARAM, identifier, requestParams);
          addParamToMapIfNotEmpty(RESUMPTION_TOKEN_PARAM, resumptionToken, requestParams);
          addParamToMapIfNotEmpty(FROM_PARAM, from, requestParams);
          addParamToMapIfNotEmpty(UNTIL_PARAM, until, requestParams);
          addParamToMapIfNotEmpty(SET_PARAM, set, requestParams);
          addParamToMapIfNotEmpty(METADATA_PREFIX_PARAM, metadataPrefix, requestParams);

          List<OAIPMHerrorType> errors = validator.validate(verb, requestParams);

          if (isNotEmpty(errors)) {
            ResponseHelper responseHelper = ResponseHelper.getInstance();
            OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, errors);
            asyncResultHandler.handle(Future.succeededFuture(responseHelper.buildFailureResponse(oaipmh, request)));
          } else {
            VerbType verbType = VerbType.fromValue(verb);
            VerbHelper verbHelper;
            if(verbType.equals(LIST_RECORDS) /*&& TODO: PREFIX = MARC21WITHHODINGS*/) {
              verbHelper = MarcWithHoldingsRequestHelper.getInstance(); //TODO: in 2020Q3 change it common approach for all helpers
            } else {
              verbHelper = HELPERS.get(verbType);
            }
            verbHelper
              .handle(request, vertxContext)
              .thenAccept(response -> {
                logger.debug(verb + " response: {}", response.getEntity());
                asyncResultHandler.handle(succeededFuture(response));
              }).exceptionally(handleError(asyncResultHandler));
          }
        } catch (Exception e) {
          asyncResultHandler.handle(getFutureWithErrorResponse());
        }
      }).exceptionally(handleError(asyncResultHandler));
  }

  private Function<Throwable, Void> handleError(Handler<AsyncResult<Response>> asyncResultHandler) {
    return throwable -> {
      asyncResultHandler.handle(getFutureWithErrorResponse());
      return null;
    };
  }

  private Future<Response> getFutureWithErrorResponse() {
    Response errorResponse = GetOaiRecordsResponse.respond500WithTextPlain(GENERIC_ERROR_MESSAGE);
    return succeededFuture(errorResponse);
  }

  private void addParamToMapIfNotEmpty(String paramName, String paramValue, Map<String, String> map) {
    if(StringUtils.isNotEmpty(paramValue)) {
      map.put(paramName, paramValue);
    }
  }

  private VerbType getVerb(String verbName) {
    boolean isVerbNameCorrect = Arrays.stream(VerbType.values())
      .anyMatch(verb -> verb.value().equals(verbName));
    return isVerbNameCorrect ? VerbType.fromValue(verbName) : VerbType.UNKNOWN;
  }

}
