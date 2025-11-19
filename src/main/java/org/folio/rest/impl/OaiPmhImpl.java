package org.folio.rest.impl;

import static io.vertx.core.Future.succeededFuture;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.FROM_PARAM;
import static org.folio.oaipmh.Constants.IDENTIFIER_PARAM;
import static org.folio.oaipmh.Constants.METADATA_PREFIX_PARAM;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_ENABLE_OAI_SERVICE;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_PARAM;
import static org.folio.oaipmh.Constants.SET_PARAM;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.openarchives.oai._2.VerbType.GET_RECORD;
import static org.openarchives.oai._2.VerbType.IDENTIFY;
import static org.openarchives.oai._2.VerbType.LIST_IDENTIFIERS;
import static org.openarchives.oai._2.VerbType.LIST_METADATA_FORMATS;
import static org.openarchives.oai._2.VerbType.LIST_RECORDS;
import static org.openarchives.oai._2.VerbType.LIST_SETS;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.AbstractHelper;
import org.folio.oaipmh.helpers.GetOaiIdentifiersHelper;
import org.folio.oaipmh.helpers.GetOaiMetadataFormatsHelper;
import org.folio.oaipmh.helpers.GetOaiRecordHelper;
import org.folio.oaipmh.helpers.GetOaiRepositoryInfoHelper;
import org.folio.oaipmh.helpers.GetOaiSetsHelper;
import org.folio.oaipmh.helpers.VerbHelper;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.mappers.PropertyNameMapper;
import org.folio.oaipmh.processors.GetListRecordsRequestHelper;
import org.folio.oaipmh.service.ConfigurationSettingsService;
import org.folio.oaipmh.validator.VerbValidator;
import org.folio.rest.jaxrs.resource.Oai;
import org.folio.spring.SpringContextUtil;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.VerbType;
import org.springframework.beans.factory.annotation.Autowired;

public class OaiPmhImpl implements Oai {

  private final Logger logger = LogManager.getLogger(OaiPmhImpl.class);

  /** Map containing OAI-PMH verb and corresponding helper instance. */
  private static final Map<VerbType, VerbHelper> HELPERS = new EnumMap<>(VerbType.class);
  
  /** Map containing configuration for each request ID. */
  private static final Map<String, JsonObject> configsMap = new HashMap<>();

  private VerbValidator validator;
  private ConfigurationSettingsService configurationService;

  public OaiPmhImpl() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  public static void init() {
    HELPERS.put(IDENTIFY, new GetOaiRepositoryInfoHelper());
    HELPERS.put(LIST_IDENTIFIERS, new GetOaiIdentifiersHelper());
    HELPERS.put(LIST_SETS, new GetOaiSetsHelper());
    HELPERS.put(LIST_METADATA_FORMATS, new GetOaiMetadataFormatsHelper());
    HELPERS.put(GET_RECORD, new GetOaiRecordHelper());
  }

  @Override
  public void getOaiRecords(String verb, String identifier, String resumptionToken,
      String from, String until, String set, String metadataPrefix,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    logger.info("getOaiRecords:: parameters verb: {}, identifier: {}, "
        + "resumptionToken: {}, from: {}, until: {}, set: {}, metadataPrefix: {}",
        verb, identifier, resumptionToken, from, until, set, metadataPrefix);
    String generatedRequestId = UUID.randomUUID().toString();
    logger.info("getOaiRecords:: Generated requestId: {}", generatedRequestId);
    Request.Builder requestBuilder = Request.builder()
        .okapiHeaders(okapiHeaders)
        .verb(getVerb(verb))
        .from(from)
        .metadataPrefix(metadataPrefix)
        .resumptionToken(resumptionToken)
        .set(set)
        .until(until)
        .identifier(identifier);
    Request request = requestBuilder.build();
    var oaipmhResponse = AbstractHelper.getResponseHelper().buildBaseOaipmhResponse(request);

    String tenantId = okapiHeaders.get(OKAPI_TENANT);
    logger.info("getOaiRecords:: Fetching configuration for tenant: {}, requestId: {}", 
        tenantId, generatedRequestId);
    configurationService.getConfigurationSettingsList(0, 100, null, tenantId)
        .onSuccess(configList -> {
          String requestId = generatedRequestId;
          try {
            logger.debug("getOaiRecords:: Successfully retrieved configuration for requestId: {}", 
                generatedRequestId);
            // Process configuration settings from database
            JsonObject config = new JsonObject();
            JsonArray configSettings = configList.getJsonArray("configurationSettings");
            if (configSettings != null) {
              logger.debug("getOaiRecords:: Processing {} configuration reId: {}", 
                  configSettings.size(), generatedRequestId);
              configSettings.stream()
                  .map(object -> (JsonObject) object)
                  .map(entry -> entry.getJsonObject("configValue"))
                  .forEach(configValue -> configValue
                    .forEach(entry -> {
                      // Map frontend key to backend key using PropertyNameMapper
                      String backendKey = PropertyNameMapper
                          .mapFrontendKeyToServerKey(entry.getKey());
                      config.put(backendKey, entry.getValue());
                    }));
            }
            configsMap.put(generatedRequestId, config);
            logger.debug("getOaiRecords:: Configuration saved for requestId: {}, config: {}", 
                generatedRequestId, config.encodePrettily());
            
            String baseUrl = getProperty(generatedRequestId, REPOSITORY_BASE_URL);
            request.setBaseUrl(baseUrl);
            logger.info("getOaiRecords:: Base URL set to: '{}' for requestId: {}", 
                baseUrl, generatedRequestId);
            if (StringUtils.isNotEmpty(identifier)) {
              String decodedIdentifier = URLDecoder.decode(identifier, "UTF-8");
              request.setIdentifier(decodedIdentifier);
              logger.info("getOaiRecords:: Identifier decoded from '{}' to '{}' for requestId: {}", 
                  identifier, decodedIdentifier, generatedRequestId);
            } else if (GET_RECORD.value().equals(verb) 
                || LIST_METADATA_FORMATS.value().equals(verb)) {
              logger.warn("getOaiRecords:: Identifier is required but not provided for verb {} "
                  + "for requestId: {}", verb, generatedRequestId);
            }
            if (!getBooleanProperty(generatedRequestId, REPOSITORY_ENABLE_OAI_SERVICE)) {
              ResponseHelper responseHelper = ResponseHelper.getInstance();
              OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request,
                  OAIPMHerrorcodeType.SERVICE_UNAVAILABLE, "OAI-PMH service is disabled");
              asyncResultHandler.handle(succeededFuture(responseHelper
                  .buildFailureResponse(oaipmh, request)));
              return;
            }

            Map<String, String> requestParams = new HashMap<>();
            addParamToMapIfNotEmpty(IDENTIFIER_PARAM, identifier, requestParams);
            addParamToMapIfNotEmpty(RESUMPTION_TOKEN_PARAM, resumptionToken, requestParams);
            addParamToMapIfNotEmpty(FROM_PARAM, from, requestParams);
            addParamToMapIfNotEmpty(UNTIL_PARAM, until, requestParams);
            addParamToMapIfNotEmpty(SET_PARAM, set, requestParams);
            addParamToMapIfNotEmpty(METADATA_PREFIX_PARAM, metadataPrefix, requestParams);

            logger.info("getOaiRecords:: Validating parameters for verb: {} params: {}, "
                + "requestId: {}", verb, requestParams, generatedRequestId);
            List<OAIPMHerrorType> errors = validator.validate(verb, requestParams, request);
            requestId = setupRequestIdIfAbsent(request, generatedRequestId);
            logger.info("getOaiRecords:: Final requestId: {} (generated: {})", 
                request.getRequestId(), generatedRequestId);
            if (isNotEmpty(errors)) {
              logger.warn("getOaiRecords:: Validation failed with {} errors for verb '{}' "
                  + "for requestId: {}", errors.size(), verb, request.getRequestId());
              for (OAIPMHerrorType error : errors) {
                logger.warn("getOaiRecords:: Validation error - code: {}, message: {} "
                    + "for requestId: {}", error.getCode(), error.getValue(), 
                    request.getRequestId());
              }
              ResponseHelper responseHelper = ResponseHelper.getInstance();
              OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, errors);
              asyncResultHandler.handle(succeededFuture(
                  responseHelper.buildFailureResponse(oaipmh, request)));
            } else {
              VerbType verbType = VerbType.fromValue(verb);
              VerbHelper verbHelper;
              if (verbType.equals(LIST_RECORDS) || verbType.equals(LIST_IDENTIFIERS)) {
                verbHelper = GetListRecordsRequestHelper.getInstance();
              } else {
                verbHelper = HELPERS.get(verbType);
              }
              logger.info("getOaiRecords:: Using helper {} for verb '{}' with requestId: {}",
                  verbHelper.getClass().getSimpleName(), verb, request.getRequestId());
              logger.info("getOaiRecords:: Calling verbHelper.handle() for requestId: {}", 
                  request.getRequestId());
              verbHelper
                  .handle(request, vertxContext)
                  .compose(response -> {
                    logger.info("getOaiRecords:: Request completed successfully for requestId: {}", 
                        request.getRequestId());
                    cleanConfigForRequestId(request.getRequestId());
                    asyncResultHandler.handle(succeededFuture(response));
                    return succeededFuture();
                  }).onFailure(e -> {
                    logger.error("getOaiRecords:: Request failed in verbHelper for requestId: {} "
                        + "with error: {}", request.getRequestId(), e.getMessage(), e);
                    cleanConfigForRequestId(request.getRequestId());
                    var responseWithErrors = AbstractHelper.buildNoRecordsFoundOaiResponse(
                        oaipmhResponse, request, e.getMessage());
                    asyncResultHandler.handle(succeededFuture(responseWithErrors));
                  });
            }
          } catch (Exception e) {
            logger.error("getOaiRecords:: RequestId {} completed with error: {}", 
                requestId, e.getMessage(), e);
            cleanConfigForRequestId(requestId);
            var responseWithErrors = AbstractHelper.buildNoRecordsFoundOaiResponse(
                oaipmhResponse, request, e.getMessage());
            asyncResultHandler.handle(succeededFuture(responseWithErrors));
          }
        }).onFailure(e -> {
          logger.error("getOaiRecords:: Failed to retrieve configuration for tenant: {} "
              + "with error: {}", tenantId, e.getMessage(), e);
          var responseWithErrors = AbstractHelper.buildNoRecordsFoundOaiResponse(
              oaipmhResponse, request, e.getMessage());
          asyncResultHandler.handle(succeededFuture(responseWithErrors));
        });
  }

  private void addParamToMapIfNotEmpty(String paramName, String paramValue,
      Map<String, String> map) {
    if (StringUtils.isNotEmpty(paramValue)) {
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
      replaceGeneratedConfigKeyWithExisted(generatedRequestId, existedRequestId);
    }
    return request.getRequestId();
  }

  private void replaceGeneratedConfigKeyWithExisted(String generatedRequestId,
      String existedRequestId) {
    configsMap.put(existedRequestId, configsMap.remove(generatedRequestId));
  }

  private String getProperty(String requestId, String name) {
    JsonObject configs = configsMap.get(requestId);
    String defaultValue = System.getProperty(name);
    if (configs != null) {
      return configs.getString(name, defaultValue);
    }
    return defaultValue;
  }

  private boolean getBooleanProperty(String requestId, String name) {
    JsonObject configs = configsMap.get(requestId);
    String defaultValue = System.getProperty(name);
    if (configs != null) {
      return Boolean.parseBoolean(configs.getString(name, defaultValue));
    }
    return Boolean.parseBoolean(defaultValue);
  }

  private void cleanConfigForRequestId(String requestId) {
    configsMap.remove(requestId);
  }

  @Autowired
  public void setValidator(VerbValidator validator) {
    this.validator = validator;
  }

  @Autowired
  public void setConfigurationService(ConfigurationSettingsService configurationService) {
    this.configurationService = configurationService;
  }
}