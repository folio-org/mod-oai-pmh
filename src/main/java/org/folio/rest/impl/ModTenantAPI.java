package org.folio.rest.impl;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.oaipmh.Constants.CONFIGS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import javax.ws.rs.core.Response;

import org.apache.http.HttpStatus;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.folio.oaipmh.mappers.PropertyNameMapper;
import org.folio.rest.client.ConfigurationsClient;
import org.folio.rest.jaxrs.model.Config;
import org.folio.rest.jaxrs.model.TenantAttributes;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class ModTenantAPI extends TenantAPI {
  private final Logger logger = LoggerFactory.getLogger(ModTenantAPI.class);

  private static final String X_OKAPI_URL = "x-okapi-url";
  private static final String X_OKAPI_TENANT = "x-okapi-tenant";
  private static final String X_OKAPI_TOKEN = "x-okapi-token";
  private static final String CONFIG_DIR_PATH = "config";
  private static final String QUERY = "module==OAIPMH+and+configName==%s";
  private static final String MODULE_NAME = "OAIPMH";
  private static final int CONFIG_JSON_BODY = 0;
  private ConfigurationHelper configurationHelper = ConfigurationHelper.getInstance();

  @Override
  public void postTenant(final TenantAttributes entity, final Map<String, String> headers,
      final Handler<AsyncResult<Response>> handlers, final Context context) {
    loadConfigData(headers, handlers);
  }

  private void loadConfigData(Map<String, String> headers, Handler<AsyncResult<Response>> handlers) {
    List<String> configsSet = Arrays.asList("behavior", "general", "technical");

    List<Future> futures = new ArrayList<>();

    configsSet.forEach(configName -> futures.add(processConfigurationByConfigName(configName, headers)));
    CompositeFuture.all(futures)
      .onComplete(future -> {
        Response response;
        if (future.succeeded()) {
          response = buildResponse(HttpStatus.SC_OK, EMPTY);
        } else {
          response = buildResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR, future.cause()
            .getMessage());
        }
        handlers.handle(Future.succeededFuture(response));
      });
  }

  private Future processConfigurationByConfigName(String configName, Map<String, String> headers) {
    Promise promise = Promise.promise();
    String okapiUrl = headers.get(X_OKAPI_URL);
    String tenant = headers.get(X_OKAPI_TENANT);
    String token = headers.get(X_OKAPI_TOKEN);

    ConfigurationsClient client = new ConfigurationsClient(okapiUrl, tenant, token);

    try {
      client.getConfigurationsEntries(format(QUERY, configName), 0, 100, null, null,
          response -> handleModConfigurationGetResponse(response, client, configName, promise));
    } catch (Exception e) {
      logger.error("Error while processing config with configName '{}'. {}", configName, e.getMessage());
      promise.fail(e);
    }
    return promise.future();
  }

  private void handleModConfigurationGetResponse(HttpClientResponse response, ConfigurationsClient client, String configName,
      Promise promise) {
    response.bodyHandler(body -> {
      JsonObject jsonConfig = body.toJsonObject();
      JsonArray configs = jsonConfig.getJsonArray(CONFIGS);
      if (configs.isEmpty()) {
        postConfig(client, configName, promise);
      } else {
        populateSystemPropertiesWithConfig(jsonConfig);
      }
    });
  }

  private void postConfig(ConfigurationsClient client, String configName, Promise promise) {
    Config config = new Config();
    config.setConfigName(configName);
    config.setEnabled(true);
    config.setModule(MODULE_NAME);
    config.setValue(getConfigValue(configName));
    try {
      client.postConfigurationsEntries(null, config, resp -> {
        if (resp.statusCode() != 201) {
          logger.error("Invalid response from mod-configuration. Cannot post config '{}': {} {}", configName, resp.statusCode(),
              resp.statusMessage());
          resp.bodyHandler(body -> {
            logger.error("Response body of post: {}", body.toJsonObject().encode());
          });
          promise.fail(new IllegalStateException("Cannot post config. " + resp.statusMessage()));
        }
      });
    } catch (Exception e) {
      promise.fail(e);
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
    String configPath = systemProperties.getProperty("configPath", CONFIG_DIR_PATH);
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

  private Response buildResponse(int statusCode, String body) {
    Response.ResponseBuilder builder = Response.status(statusCode);
    if (!body.isEmpty()) {
      builder = builder.header(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.TEXT_PLAIN.toString())
        .entity(body);
    }
    return builder.build();
  }

}
