package org.folio.rest.impl;

import io.vertx.core.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.*;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.processors.MarcWithHoldingsRequestHelper;
import org.folio.oaipmh.validator.VerbValidator;
import org.folio.rest.jaxrs.resource.Oai;
import org.folio.spring.SpringContextUtil;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.VerbType;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.net.URLDecoder;
import java.util.*;

import static io.vertx.core.Future.succeededFuture;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.*;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getProperty;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.VerbType.*;

public class OaiPmhImpl implements Oai {

  private final Logger logger = LogManager.getLogger(OaiPmhImpl.class);

  /** Map containing OAI-PMH verb and corresponding helper instance. */
  private static final Map<VerbType, VerbHelper> HELPERS = new EnumMap<>(VerbType.class);

  private VerbValidator validator;

  public OaiPmhImpl() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  public static void init() {
    HELPERS.put(IDENTIFY, new GetOaiRepositoryInfoHelper());
    HELPERS.put(LIST_IDENTIFIERS, new GetOaiIdentifiersHelper());
    HELPERS.put(LIST_RECORDS, new GetOaiRecordsHelper());
    HELPERS.put(LIST_SETS, new GetOaiSetsHelper());
    HELPERS.put(LIST_METADATA_FORMATS, new GetOaiMetadataFormatsHelper());
    HELPERS.put(GET_RECORD, new GetOaiRecordHelper());
  }

  @Override
  public void getOaiRecords(String verb, String identifier, String resumptionToken,
                            String from, String until, String set, String metadataPrefix,
                            Map<String, String> okapiHeaders,
                            Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    logger.info("getOaiRecords:: parameters verb: {}, identifier: {}, resumptionToken: {}, from: {}, until: {}, set: {}, metadataPrefix: {}",
      verb, identifier, resumptionToken, from, until, set, metadataPrefix);
    String generatedRequestId = UUID.randomUUID().toString();
    RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, generatedRequestId)
      .onSuccess(v -> {
        String requestId = generatedRequestId;
        try {
          Request.Builder requestBuilder = Request.builder()
            .okapiHeaders(okapiHeaders)
            .verb(getVerb(verb))
            .baseURL(getProperty(generatedRequestId, REPOSITORY_BASE_URL))
            .from(from).metadataPrefix(metadataPrefix).resumptionToken(resumptionToken).set(set).until(until);
          if (StringUtils.isNotEmpty(identifier)) {
            requestBuilder.identifier(URLDecoder.decode(identifier, "UTF-8"));
          }

          Request request = requestBuilder.build();

          if (!getBooleanProperty(generatedRequestId, REPOSITORY_ENABLE_OAI_SERVICE)) {
            ResponseHelper responseHelper = ResponseHelper.getInstance();
            OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, OAIPMHerrorcodeType.SERVICE_UNAVAILABLE, "OAI-PMH service is disabled");
            asyncResultHandler.handle(succeededFuture(responseHelper.buildFailureResponse(oaipmh, request)));
            return;
          }

          Map<String, String> requestParams = new HashMap<>();
          addParamToMapIfNotEmpty(IDENTIFIER_PARAM, identifier, requestParams);
          addParamToMapIfNotEmpty(RESUMPTION_TOKEN_PARAM, resumptionToken, requestParams);
          addParamToMapIfNotEmpty(FROM_PARAM, from, requestParams);
          addParamToMapIfNotEmpty(UNTIL_PARAM, until, requestParams);
          addParamToMapIfNotEmpty(SET_PARAM, set, requestParams);
          addParamToMapIfNotEmpty(METADATA_PREFIX_PARAM, metadataPrefix, requestParams);

          List<OAIPMHerrorType> errors = validator.validate(verb, requestParams, request);
          requestId = setupRequestIdIfAbsent(request, generatedRequestId);
          if (isNotEmpty(errors)) {
            logger.info("getOaiRecords:: Validation errors {} for verb {} for requestId {}", errors.size(), verb, request.getRequestId());
            ResponseHelper responseHelper = ResponseHelper.getInstance();
            OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, errors);
            asyncResultHandler.handle(succeededFuture(responseHelper.buildFailureResponse(oaipmh, request)));
          } else {
            VerbType verbType = VerbType.fromValue(verb);
            VerbHelper verbHelper;

            String targetMetadataPrefix = request.getMetadataPrefix();

            if((verbType.equals(LIST_RECORDS) || verbType.equals(LIST_IDENTIFIERS)) && MetadataPrefix.MARC21WITHHOLDINGS.getName().equals(targetMetadataPrefix)) {
              verbHelper = MarcWithHoldingsRequestHelper.getInstance();
            } else {
              verbHelper = HELPERS.get(verbType);
            }
            logger.info("getOaiRecords:: Use {} for requestId {}", verbHelper.getClass().getCanonicalName(),  request.getRequestId());
            verbHelper
              .handle(request, vertxContext)
              .compose(response -> {
                RepositoryConfigurationUtil.cleanConfigForRequestId(request.getRequestId());
                asyncResultHandler.handle(succeededFuture(response));
                return succeededFuture();
              }).onFailure(t-> {
                RepositoryConfigurationUtil.cleanConfigForRequestId(request.getRequestId());
                asyncResultHandler.handle(getFutureWithErrorResponse(t, request));
               });
          }
        } catch (Exception e) {
          logger.error("getOaiRecords:: RequestId {} completed with  error {}", requestId,  e.getMessage());
          RepositoryConfigurationUtil.cleanConfigForRequestId(requestId);
          asyncResultHandler.handle(getFutureWithErrorResponse(e.getMessage()));
        }
      }).onFailure(throwable -> asyncResultHandler.handle(getFutureWithErrorResponse(throwable.getMessage())));
  }

  private Future<Response> getFutureWithErrorResponse(Throwable t, Request request) {
    final Response errorResponse;
    logger.error("getOaiRecords::  RequestId {} completed with error {}", request.getRequestId(), t.getMessage());
    if (t instanceof IllegalArgumentException) {
      final ResponseHelper rh = ResponseHelper.getInstance();
      OAIPMH oaipmh = rh.buildOaipmhResponseWithErrors(request, BAD_RESUMPTION_TOKEN, RESUMPTION_TOKEN_FORMAT_ERROR);
      errorResponse = rh.buildFailureResponse(oaipmh, request);
    } else {
      errorResponse = GetOaiRecordsResponse.respond500WithTextPlain(t.getMessage());
    }
    return succeededFuture(errorResponse);
  }

  private Future<Response> getFutureWithErrorResponse(String errorMessage) {
    Response errorResponse = GetOaiRecordsResponse.respond500WithTextPlain(errorMessage);
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

  private String setupRequestIdIfAbsent(Request request, String generatedRequestId) {
    if (StringUtils.isEmpty(request.getRequestId())) {
      request.setRequestId(generatedRequestId);
    } else {
      String existedRequestId = request.getRequestId();
      RepositoryConfigurationUtil.replaceGeneratedConfigKeyWithExisted(generatedRequestId, existedRequestId);
    }
    return request.getRequestId();
  }

  @Autowired
  public void setValidator(VerbValidator validator) {
    this.validator = validator;
  }
}
