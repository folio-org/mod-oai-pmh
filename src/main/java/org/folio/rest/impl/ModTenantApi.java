package org.folio.rest.impl;

import static java.lang.String.format;
import static org.folio.oaipmh.Constants.CONFIGS;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.OKAPI_URL;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.folio.oaipmh.mappers.PropertyNameMapper;
import org.folio.oaipmh.service.ConfigurationSettingsService;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.client.ConfigurationsClient;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.client.exceptions.ResponseException;
import org.folio.spring.SpringContextUtil;
import org.glassfish.jersey.message.internal.Statuses;
import org.springframework.beans.factory.annotation.Autowired;

public class ModTenantApi extends TenantAPI {

  private final Logger logger = LogManager.getLogger(ModTenantApi.class);

  private static final String CONFIG_DIR_PATH = "config";
  private static final String QUERY = "module==OAIPMH and configName==%s";
  private static final String CONFIG_PATH_KEY = "configPath";

  private ConfigurationHelper configurationHelper;
  private ConfigurationSettingsService configurationSettingsService;

  public ModTenantApi() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void postTenant(final TenantAttributes entity, final Map<String, String> headers,
      final Handler<AsyncResult<Response>> handlers, final Context context) {
    super.postTenant(entity, headers, postTenantAsyncResultHandler -> {
      if (postTenantAsyncResultHandler.failed()) {
        handlers.handle(postTenantAsyncResultHandler);
      } else {
        List<String> configsSet = Arrays.asList("behavioral", "general", "technical");
        loadConfigurationData(headers, configsSet).onComplete(asyncResult -> {
          if (asyncResult.succeeded()) {
            handlers.handle(Future.succeededFuture(buildSuccessResponse(asyncResult.result())));
          } else {
            logger.error(asyncResult.cause());
            handlers.handle(Future.failedFuture(
                new ResponseException(buildErrorResponse(asyncResult.cause().getMessage()))));
          }
        });
      }
    }, context);
  }

  @Override
  Future<Integer> loadData(TenantAttributes attributes, String tenantId,
      Map<String, String> headers, Context vertxContext) {
    return super.loadData(attributes, tenantId, headers, vertxContext).compose(num -> {
      Vertx vertx = vertxContext.owner();
      LiquibaseUtil.initializeSchemaForTenant(vertx, tenantId);
      return Future.succeededFuture(num);
    });
  }

  public Future<String> loadConfigurationData(Map<String, String> headers,
      List<String> configsSet) {
    String okapiUrl = headers.get(OKAPI_URL);
    String tenant = headers.get(OKAPI_TENANT);
    String token = headers.get(OKAPI_TOKEN);

    WebClient webClient = WebClientProvider.getWebClient();
    ConfigurationsClient client = new ConfigurationsClient(okapiUrl, tenant, token, webClient);

    List<Future<String>> futures = new ArrayList<>();

    configsSet.forEach(configName ->
        futures.add(processConfigurationByConfigName(configName,
            client, tenant, headers.get(XOkapiHeaders.USER_ID))));
    return GenericCompositeFuture.all(futures)
        .map("Configuration has been set up successfully.")
        .recover(throwable -> {
          if (throwable.getMessage() == null) {
            throwable = new RuntimeException(
                "Error has been occurred while communicating to mod-configuration", throwable);
          }
          return Future.failedFuture(throwable);
        });
  }

  private Future<String> processConfigurationByConfigName(String configName,
      ConfigurationsClient client, String tenantId, String userId) {

    // First, check if configuration already exists in mod-oai-pmh configuration table
    return configurationSettingsService.getConfigurationSettingsByName(configName, tenantId)
        .compose(existingConfig -> {
          // Configuration already exists, skip repopulating
          logger.info("Configuration {} already exists in mod-oai-pmh, skipping", configName);
          return Future.succeededFuture("Configuration already exists");
        })
        .recover(throwable -> {
          // Configuration doesn't exist, proceed with migration/default insertion
          if (throwable instanceof NotFoundException) {
            Promise<String> promise = Promise.promise();

            // Try to get configuration from mod-configuration
            client.getConfigurationsEntries(
                format(QUERY, configName), 0, 100, null, null, result -> {
                  if (result.succeeded()) {
                    HttpResponse<Buffer> response = result.result();
                    JsonObject body = response.bodyAsJsonObject();
                    JsonArray configs = body.getJsonArray(CONFIGS);

                    if (!configs.isEmpty()) {
                      // Configuration found in mod-configuration, migrate it
                      JsonObject configEntry = configs.getJsonObject(0);
                      JsonObject valueJson = new JsonObject(configEntry.getString("value"));

                      saveToConfigurationSettings(configName, valueJson, tenantId, userId)
                          .onSuccess(v -> {
                            logger.info("Migrated config {} from mod-configuration", configName);
                            promise.complete("Configuration migrated");
                          })
                          .onFailure(promise::fail);
                    } else {
                      // Configuration not found in mod-configuration, use default values
                      JsonObject defaultConfig = new JsonObject(getConfigValue(configName));
                      saveToConfigurationSettings(configName, defaultConfig, tenantId, userId)
                          .onSuccess(v -> {
                            logger.info("Inserted default config {}", configName);
                            promise.complete("Default configuration inserted");
                          })
                          .onFailure(promise::fail);
                    }
                  } else {
                    // Error communicating with mod-configuration, use default values
                    logger.warn("Failed to communicate with mod-configuration for {}, "
                        + "using defaults: {}",
                        configName, result.cause().getMessage());
                    JsonObject defaultConfig = new JsonObject(getConfigValue(configName));
                    saveToConfigurationSettings(configName, defaultConfig, tenantId, userId)
                        .onSuccess(v -> {
                          logger.info("Inserted default config {} "
                              + "after mod-configuration error", configName);
                          promise.complete("Default configuration inserted");
                        })
                        .onFailure(promise::fail);
                  }
                });
            return promise.future();
          }
          return Future.failedFuture(throwable);
        });
  }

  /**.
   * Saves configuration settings to the database.
   *
   * @param configName - name of the configuration
   * @param configValue - JSON configuration values
   * @param tenantId - tenant identifier
   * @param userId - user identifier
   * @return Future with the saved configuration or error
   */
  private Future<JsonObject> saveToConfigurationSettings(String configName,
      JsonObject configValue, String tenantId, String userId) {
    JsonObject entry = new JsonObject()
        .put("configName", configName)
        .put("configValue", configValue);

    return configurationSettingsService.saveConfigurationSettings(entry, tenantId, userId);
  }

  /**
   * Composes the json which contains configuration keys values. If some of configurations
   * have been already specified via JVM then
   * such values will be used and further posted instead of defaults.
   *
   * @param configName - json file with default configurations that is placed under the
   *                   resource folder.
   * @return string representation of json object
   */
  private String getConfigValue(String configName) {
    Properties systemProperties = System.getProperties();
    String configPath = systemProperties.getProperty(CONFIG_PATH_KEY, CONFIG_DIR_PATH);
    JsonObject jsonConfigEntry = configurationHelper.getJsonConfigFromResources(configPath,
        configName + ".json");
    Map<String, String> configKeyValueMap =
        configurationHelper.getConfigKeyValueMapFromJsonEntryValueField(jsonConfigEntry);
    JsonObject configEntryValueField = new JsonObject();
    configKeyValueMap.forEach((key, configDefaultValue) -> {
      String possibleJvmSpecifiedValue = systemProperties.getProperty(key);
      if (Objects.nonNull(possibleJvmSpecifiedValue)
          && !possibleJvmSpecifiedValue.equals(configDefaultValue)) {
        configEntryValueField.put(PropertyNameMapper.mapToFrontendKeyName(key),
            possibleJvmSpecifiedValue);
      } else {
        configEntryValueField.put(PropertyNameMapper.mapToFrontendKeyName(key),
            configDefaultValue);
      }
    });
    return configEntryValueField.encode();
  }

  private Response buildSuccessResponse(String body) {
    Response.ResponseBuilder builder = Response.status(HttpStatus.SC_OK)
        .header(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.TEXT_PLAIN.toString())
        .entity(body);
    return builder.build();
  }

  private Response buildErrorResponse(String info) {
    Response.ResponseBuilder builder = Response.status(Statuses.from(400, info));
    return builder.build();
  }

  @Autowired
  public void setConfigurationHelper(ConfigurationHelper configurationHelper) {
    this.configurationHelper = configurationHelper;
  }

  @Autowired
  public void setConfigurationSettingsService(
      ConfigurationSettingsService configurationSettingsService) {
    this.configurationSettingsService = configurationSettingsService;
  }

}
