package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.exception.CleanUpErrorLogsException;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.folio.oaipmh.mappers.PropertyNameMapper;
import org.folio.oaipmh.service.ErrorsService;
import org.folio.oaipmh.service.InstancesService;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.client.ConfigurationsClient;
import org.folio.rest.jaxrs.model.Config;
import org.folio.rest.jaxrs.resource.OaiPmhCleanUpErrorLogs;
import org.folio.rest.jaxrs.resource.OaiPmhCleanUpInstances;
import org.folio.s3.client.FolioS3Client;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.io.File;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static java.lang.String.format;
import static org.folio.oaipmh.Constants.CONFIGS;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.OKAPI_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_FETCHING_CLEAN_ERRORS_INTERVAL;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getProperty;
import static org.folio.rest.jaxrs.resource.OaiPmhCleanUpInstances.PostOaiPmhCleanUpInstancesResponse.respond500WithTextPlain;

public class CleanUpErrorLogs implements OaiPmhCleanUpErrorLogs {

  private final Logger logger = LogManager.getLogger(this.getClass());

  @Autowired
  private FolioS3Client folioS3Client;

  @Autowired
  private InstancesService instancesService;

  @Autowired
  private ErrorsService errorsService;

  @Autowired
  private ConfigurationHelper configurationHelper;

  private static final String QUERY = "module==OAIPMH and configName==%s";
  private static final String MODULE_NAME = "OAIPMH";
  private static final String CONFIG_PATH_KEY = "configPath";
  private static final String CONFIG_DIR_PATH = "config";
  private static final int CONFIG_JSON_BODY = 0;
  private static final String Load_Tech_Configs_Error_Message = "fail due loadTechnicalConfigs";

  public CleanUpErrorLogs() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void postOaiPmhCleanUpErrorLogs(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    logger.debug("Running cleaning up error logs");
    final long[] cleanInterval = {30};

    loadTechnicalConfigs(okapiHeaders).onComplete(asyncResult -> cleanInterval[0] =
      Long.parseLong(getProperty("", REPOSITORY_FETCHING_CLEAN_ERRORS_INTERVAL)));

    OffsetDateTime offsetDateTime = ZonedDateTime
      .ofInstant(Instant.now(), ZoneId.systemDefault())
      .minusDays(cleanInterval[0])
      .toOffsetDateTime();

    var tenant = okapiHeaders.get(OKAPI_TENANT);

    vertxContext.runOnContext(v ->
      instancesService.getRequestMetadataIdsByStartedDateAndExistsByPathToErrorFileInS3(tenant, offsetDateTime)
        .onComplete(result -> {
          if (result.succeeded()) {
            if (!result.result().isEmpty()) {
              result.result().forEach(id -> {
                try {
                  instancesService.updateRequestMetadataByPathToError(id, tenant, "");
                } catch (Exception ex) {
                  logger.error("error while updateRequestMetadataByPathToError : requestId: {}", id);
                }
                try {
                  instancesService.updateRequestMetadataByLinkToError(id, tenant, "");
                } catch (Exception ex) {
                  logger.error("error while updateRequestMetadataByLinkToError : requestId: {}", id);
                }
                try {
                  folioS3Client.remove(File.separator + id + "-error.csv");
                } catch (Exception ex) {
                  logger.error("error while deleting file from S3: fileName: {}", File.separator + id + "-error.csv");
                }
                try {
                  errorsService.deleteErrorsByRequestId(tenant, id);
                } catch (Exception ex) {
                  logger.error("error while deleteErrorsByRequestId: requestId: {}", id);
                }
              });
            } else {
              logger.debug("nothing to clean (error logs)");
            }
          } else {
            logger.error("error while selecting getRequestMetadataIdsByStartedDateAndExistsByPathToErrorFileInS3");
          }
        })
        .map(OaiPmhCleanUpInstances.PostOaiPmhCleanUpInstancesResponse.respond204())
        .map(Response.class::cast)
        .otherwise(throwable -> respond500WithTextPlain(throwable.getMessage()))
        .onComplete(asyncResultHandler));
  }

  private Future<String> loadTechnicalConfigs(Map<String, String> headers) {
    try {
      String configName = "technical";

      String okapiUrl = headers.get(OKAPI_URL);
      String tenant = headers.get(OKAPI_TENANT);
      String token = headers.get(OKAPI_TOKEN);

      WebClient webClient = WebClientProvider.getWebClient();
      ConfigurationsClient client = new ConfigurationsClient(okapiUrl, tenant, token, webClient);

      List<Future<String>> futures = new ArrayList<>();

      Promise<String> promise = Promise.promise();

      client.getConfigurationsEntries(format(QUERY, configName), 0, 100, null, null, result -> {
        if (result.succeeded()) {
          HttpResponse<Buffer> response = result.result();

          if (response.statusCode() != 200) {
            throw new CleanUpErrorLogsException(Load_Tech_Configs_Error_Message);
          }
          JsonObject body = response.bodyAsJsonObject();
          JsonArray configs = body.getJsonArray(CONFIGS);
          if (configs.isEmpty()) {
            try {
              Config config = new Config();
              config.setConfigName(configName);
              config.setEnabled(true);
              config.setModule(MODULE_NAME);

              Properties systemProperties = System.getProperties();
              String configPath = systemProperties.getProperty(CONFIG_PATH_KEY, CONFIG_DIR_PATH);
              JsonObject jsonConfigEntry = configurationHelper.getJsonConfigFromResources(configPath, configName + ".json");
              Map<String, String> configKeyValueMap = configurationHelper.getConfigKeyValueMapFromJsonEntryValueField(jsonConfigEntry);
              JsonObject configEntryValueField = new JsonObject();
              configKeyValueMap.forEach((key, configDefaultValue) -> {
                String possibleJvmSpecifiedValue = systemProperties.getProperty(key);
                if (Objects.nonNull(possibleJvmSpecifiedValue) && !possibleJvmSpecifiedValue.equals(configDefaultValue)) {
                  configEntryValueField.put(PropertyNameMapper.mapToFrontendKeyName(key), possibleJvmSpecifiedValue);
                } else {
                  configEntryValueField.put(PropertyNameMapper.mapToFrontendKeyName(key), configDefaultValue);
                }
              });

              config.setValue(configEntryValueField.encode());

              client.postConfigurationsEntries(null, config, resultEntries -> {
                if (resultEntries.failed()) {
                  throw new CleanUpErrorLogsException(Load_Tech_Configs_Error_Message);
                }
                HttpResponse<Buffer> httpResponse = resultEntries.result();
                if (httpResponse.statusCode() != 201) {
                  throw new CleanUpErrorLogsException(Load_Tech_Configs_Error_Message);
                }
                promise.complete();
              });
            } catch (Exception e) {
              throw new CleanUpErrorLogsException(Load_Tech_Configs_Error_Message);
            }
          } else {
            JsonObject configBody = body.getJsonArray(CONFIGS)
              .getJsonObject(CONFIG_JSON_BODY);
            Map<String, String> configKeyValueMap = configurationHelper.getConfigKeyValueMapFromJsonEntryValueField(configBody);
            Properties sysProps = System.getProperties();
            sysProps.putAll(configKeyValueMap);
            promise.complete();
          }
        } else {
          throw new CleanUpErrorLogsException(Load_Tech_Configs_Error_Message);
        }
      });

      var promiseConfig = promise.future();
      futures.add(promiseConfig);

      return GenericCompositeFuture.all(futures)
        .map("Configuration has been set up successfully.")
        .recover(throwable -> {
          if (throwable.getMessage() == null) {
            throwable = new RuntimeException("Error has been occurred while communicating to mod-configuration", throwable);
          }
          return Future.failedFuture(throwable);
        });
    } catch (Exception ex) {
      return Future.failedFuture("failing due loadTechnicalConfigs: " + ex.getMessage());
    }
  }
}
