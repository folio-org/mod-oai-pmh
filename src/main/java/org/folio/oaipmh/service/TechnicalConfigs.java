package org.folio.oaipmh.service;

import io.vertx.core.Future;
import io.vertx.core.Promise;
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
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.client.ConfigurationsClient;
import org.folio.rest.jaxrs.model.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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

@Service
public class TechnicalConfigs {
  protected final Logger logger = LogManager.getLogger(getClass());

  @Autowired
  private ConfigurationHelper configurationHelper;

  private static final String QUERY = "module==OAIPMH and configName==%s";
  private static final String MODULE_NAME = "OAIPMH";
  private static final String CONFIG_PATH_KEY = "configPath";
  private static final String CONFIG_DIR_PATH = "config";
  private static final int CONFIG_JSON_BODY = 0;
  private static final String Load_Tech_Configs_Error_Message = "fail due loadTechnicalConfigs";

  public Future<String> loadConfigs(Map<String, String> headers) {
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
