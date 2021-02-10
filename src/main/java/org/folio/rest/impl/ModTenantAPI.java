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
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.rest.client.ConfigurationsClient;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.folio.oaipmh.mappers.PropertyNameMapper;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.jaxrs.model.Config;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

public class ModTenantAPI extends TenantAPI {
  private final Logger logger = LogManager.getLogger(ModTenantAPI.class);

  private static final String CONFIG_DIR_PATH = "config";
  private static final String QUERY = "module==OAIPMH and configName==%s";
  private static final String MODULE_NAME = "OAIPMH";
  private static final int CONFIG_JSON_BODY = 0;

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
        GenericCompositeFuture.all(Arrays.asList(loadConfigurationDataFuture, initDatabaseFuture))
          .onComplete(future -> {
            String message;
            if (future.succeeded()) {
              message = loadConfigurationDataFuture.result() + " " + initDatabaseFuture.result();
            } else {
              message = getResultErrorMessage(loadConfigurationDataFuture, initDatabaseFuture);
            }
            handlers.handle(Future.succeededFuture(buildResponse(message)));
          });
      }
    }, context);
  }

  private Future<String> loadConfigurationData(Map<String, String> headers) {
    Promise<String> promise = Promise.promise();
    List<String> configsSet = Arrays.asList("behavior", "general", "technical");

    String okapiUrl = headers.get(OKAPI_URL);
    String tenant = headers.get(OKAPI_TENANT);
    String token = headers.get(OKAPI_TOKEN);

    ConfigurationsClient client = new ConfigurationsClient(okapiUrl, tenant, token);

    List<Future> futures = new ArrayList<>();

    configsSet.forEach(configName -> futures.add(processConfigurationByConfigName(configName, client)));
    GenericCompositeFuture.all(futures)
      .onComplete(future -> {
        String message;
        if (future.succeeded()) {
          message = "Configurations has been set up successfully";
        } else {
          message = Optional.ofNullable(future.cause())
            .map(Throwable::getMessage)
            .orElse("Error has been occurred while communicating to mod-configuration");
        }
        promise.complete(message);
      });
    return promise.future();
  }

  private Future<String> processConfigurationByConfigName(String configName, ConfigurationsClient client) {
    Promise<String> promise = Promise.promise();
    try {
      logger.info(String.format("Getting configurations with configName = '%s'", configName));
      Promise<HttpResponse<Buffer>> responsePromise = Promise.promise();
      client.getConfigurationsEntries(format(QUERY, configName), 0, 100, null, null, responsePromise);
      responsePromise.future()
        .onSuccess(response -> handleModConfigurationGetResponse(response, client, configName, promise))
        .onFailure(e -> {
          logger.error(String.format("Error while processing config with configName '%s'. '%s'", configName, e.getMessage()), e);
          promise.fail(e.getMessage());
        });
    } catch (Exception e) {
      logger.error(String.format("Error while processing config with configName '%s'. '%s'", configName, e.getMessage()), e);
      promise.fail(e.getMessage());
    }
    return promise.future();
  }

  private void handleModConfigurationGetResponse(HttpResponse<Buffer> response, ConfigurationsClient client, String configName,
                                                 Promise<String> promise) {
    if (response.statusCode() != 200) {
      logger.error(response.bodyAsBuffer().toString());
      promise.fail(response.bodyAsBuffer().toString());
      return;
    }
    JsonObject jsonConfig = response.bodyAsJsonObject();
    JsonArray configs = jsonConfig.getJsonArray(CONFIGS);
    if (configs.isEmpty()) {
      logger.info("Configuration group with configName {} isn't exist. " + "Posting default configs for {} configuration group",
        MODULE_NAME, configName);
      postConfig(client, configName, promise);
    } else {
      logger.info("Configurations has been got successfully, applying configurations to module system properties");
      populateSystemPropertiesWithConfig(jsonConfig);
      promise.complete();
    }
  }

  private void postConfig(ConfigurationsClient client, String configName, Promise<String> promise) {
    Config config = new Config();
    config.setConfigName(configName);
    config.setEnabled(true);
    config.setModule(MODULE_NAME);
    config.setValue(getConfigValue(configName));
    try {
      client.postConfigurationsEntries(null, config, respAr -> {
        if (respAr.succeeded()) {
          HttpResponse<Buffer> resp = respAr.result();
          if (resp.statusCode() != 201) {
            logger
              .error(String.format("Invalid response from mod-configuration. Cannot post config '%s'." + " Response message: : %s:%s",
                configName, resp.statusCode(), resp.statusMessage()));
            promise.fail("Cannot post config. " + resp.statusMessage());
          }
        } else if(respAr.failed()) {
          logger.error(respAr.cause().getMessage(), respAr.cause());
          promise.fail(respAr.cause().getMessage());
        }
      });
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      promise.fail(e.getMessage());
      return;
    }
    promise.complete();
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
        logger.error(message);
        promise.fail(message);
      }
    });
    return promise.future();
  }

  /**
   * Returns error message of the first failed future. Since both futures required to be succeeded for enabling module for tenant
   * then if one of them fails then there are no matter that the second future will be succeeded and therefore we don't need to wait
   * until it will be completed and thus we should respond with message of the first failed future.
   *
   * @param configDataFuture - future of loading configuration data
   * @param initDbFuture     - future of initializing the module database
   * @return error message
   */
  private String getResultErrorMessage(Future<String> configDataFuture, Future<String> initDbFuture) {
    if (configDataFuture.failed()) {
      return configDataFuture.result();
    }
    return initDbFuture.result();
  }

  private Response buildResponse(String body) {
    Response.ResponseBuilder builder = Response.status(HttpStatus.SC_OK)
      .header(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.TEXT_PLAIN.toString())
      .entity(body);
    return builder.build();
  }

  @Autowired
  public void setConfigurationHelper(ConfigurationHelper configurationHelper) {
    this.configurationHelper = configurationHelper;
  }

}
