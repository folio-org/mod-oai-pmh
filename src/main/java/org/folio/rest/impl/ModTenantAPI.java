package org.folio.rest.impl;

import static java.lang.String.format;
import static org.folio.oaipmh.Constants.CONFIGS;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.OKAPI_URL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import javax.ws.rs.core.Response;

import org.apache.http.HttpStatus;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.folio.oaipmh.mappers.PropertyNameMapper;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.client.ConfigurationsClient;
import org.folio.rest.jaxrs.model.Config;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.client.exceptions.ResponseException;
import org.folio.rest.tools.utils.VertxUtils;
import org.folio.spring.SpringContextUtil;
import org.glassfish.jersey.message.internal.Statuses;
import org.springframework.beans.factory.annotation.Autowired;

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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

public class ModTenantAPI extends TenantAPI {

  private final Logger logger = LogManager.getLogger(ModTenantAPI.class);

  private static final String CONFIG_DIR_PATH = "config";
  private static final String QUERY = "module==OAIPMH and configName==%s";
  private static final String MODULE_NAME = "OAIPMH";
  private static final int CONFIG_JSON_BODY = 0;

  static final String CONFIG_PATH_KEY = "configPath";

  private ConfigurationHelper configurationHelper;

  public ModTenantAPI() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void postTenant(final TenantAttributes entity, final Map<String, String> headers,
      final Handler<AsyncResult<Response>> handlers, final Context context) {
    super.postTenant(entity, headers, postTenantAsyncResultHandler -> {
      if (postTenantAsyncResultHandler.failed()) {
        handlers.handle(postTenantAsyncResultHandler);
      } else {
        Future<String> loadConfigurationDataFuture = loadConfigurationData(headers);
        Future<String> initDatabaseFuture = initDatabase(headers, context.owner());
        GenericCompositeFuture.all(List.of(loadConfigurationDataFuture, initDatabaseFuture))
          .onComplete(future -> {
            if (future.succeeded()) {
              String message = loadConfigurationDataFuture.result() + " " + initDatabaseFuture.result();
              handlers.handle(Future.succeededFuture(buildResponse(message)));
            } else {
              String message = future.cause().getMessage();
              logger.error(message, future.cause());
              handlers.handle(Future.failedFuture(new ResponseException(buildErrorResponse(message))));
            }
          });
      }
    }, context);
  }

  private Future<String> loadConfigurationData(Map<String, String> headers) {
    List<String> configsSet = Arrays.asList("behavior", "general", "technical");

    String okapiUrl = headers.get(OKAPI_URL);
    String tenant = headers.get(OKAPI_TENANT);
    String token = headers.get(OKAPI_TOKEN);

    WebClient webClient = WebClient.create(VertxUtils.getVertxFromContextOrNew());
    ConfigurationsClient client = new ConfigurationsClient(okapiUrl, tenant, token, webClient);

    List<Future<String>> futures = new ArrayList<>();

    configsSet.forEach(configName -> futures.add(processConfigurationByConfigName(configName, client)));
    return GenericCompositeFuture.all(futures)
      .map("Configuration has been set up successfully")
      .recover(e -> {
        if (e.getMessage() == null) {
          e = new RuntimeException("Error has been occurred while communicating to mod-configuration", e);
        }
        return Future.failedFuture(e);
      });
  }

  private Future<String> processConfigurationByConfigName(String configName, ConfigurationsClient client) {
    Promise<String> promise = Promise.promise();
    try {
      logger.info("Getting configurations with configName \"{}\"", configName);
      client.getConfigurationsEntries(format(QUERY, configName), 0, 100, null, null, result -> {
        if (result.succeeded()) {
          HttpResponse<Buffer> response = result.result();
          handleModConfigurationGetResponse(response, client, configName, promise);
        } else {
          String message = "POST request to mod-configuration with config: " + configName + ", has failed.";
          logger.error(message, result.cause());
          promise.fail(new IllegalStateException(message, result.cause()));
        }
      });
    } catch (Exception e) {
      String message = String.format("Error while processing config with configName '%s'. '%s'", configName, e.getMessage());
      logger.error(message, e);
      promise.fail(new IllegalStateException(message, e));
    }
    return promise.future();
  }

  private void handleModConfigurationGetResponse(HttpResponse<Buffer> response, ConfigurationsClient client, String configName,
      Promise<String> promise) {
    if (response.statusCode() != 200) {
      Buffer buffer = response.body();
      logger.error(buffer.toString());
      promise.fail(new IllegalStateException("Invalid GET request response returned for config with name: " + configName + "; response: " + buffer.toString()));
      return;
    }
    JsonObject body = response.bodyAsJsonObject();
      JsonArray configs = body.getJsonArray(CONFIGS);
      if (configs.isEmpty()) {
        logger.info("Configuration group with configName {} doesn't exist. Posting default configs for {} configuration group.",
            MODULE_NAME, configName);
        postConfig(client, configName, promise);
      } else {
        logger.info("Configurations has been got successfully, applying configurations to module system properties.");
        populateSystemPropertiesWithConfig(body);
        promise.complete();
      }
  }

  private void postConfig(ConfigurationsClient client, String configName, Promise<String> promise) {
    try {
      Config config = new Config();
      config.setConfigName(configName);
      config.setEnabled(true);
      config.setModule(MODULE_NAME);
      config.setValue(getConfigValue(configName));
      client.postConfigurationsEntries(null, config, result -> {
        if (result.failed()) {
          Exception e = new IllegalStateException("Error occurred during config posting.", result.cause());
          logger.error(e.getMessage(), e);
          promise.fail(e);
          return;
        }
        HttpResponse<Buffer> response = result.result();
        if (response.statusCode() != 201) {
          logger.error("Invalid responseonse from mod-configuration. Cannot post config '{}'. Response message: {} {}",
              configName, response.statusCode(), response.statusMessage());
          promise.fail(new IllegalStateException("Cannot post config. " + response.statusMessage()));
          return;
        }
        logger.info("Config {} posted successfully.", configName);
        promise.complete();
      });
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      promise.fail(e);
      return;
    }
  }

  private void populateSystemPropertiesWithConfig(JsonObject jsonResponse) {
    JsonObject configBody = jsonResponse.getJsonArray(CONFIGS)
      .getJsonObject(CONFIG_JSON_BODY);
    Map<String, String> configKeyValueMap = configurationHelper.getConfigKeyValueMapFromJsonEntryValueField(configBody);
    Properties sysProps = System.getProperties();
    sysProps.putAll(configKeyValueMap);
  }

  /**
   * Composes the json which contains configuration keys values. If some of configurations have been already specified via JVM then
   * such values will be used and further posted instead of defaults.
   *
   * @param configName - json file with default configurations that is placed under the resource folder.
   * @return string representation of json object
   */
  private String getConfigValue(String configName) {
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
    return configEntryValueField.encode();
  }

  private Future<String> initDatabase(Map<String, String> okapiHeaders, Vertx vertx) {
    Promise<String> promise = Promise.promise();
    String tenantId = okapiHeaders.get(OKAPI_TENANT);
    vertx.executeBlocking(blockingFeature -> {
      LiquibaseUtil.initializeSchemaForTenant(vertx, tenantId);
      blockingFeature.complete();
    }, result -> {
      if (result.succeeded()) {
        String message = "The database has been initialized successfully";
        logger.info(message);
        promise.complete(message);
      } else {
        String message = Optional.ofNullable(result.cause())
          .map(Throwable::getMessage)
          .orElse("Failed to initialize the database");
        logger.error(message, result.cause());
        promise.fail(new RuntimeException(message, result.cause()));
      }
    });
    return promise.future();
  }

  private Response buildResponse(String body) {
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

}
