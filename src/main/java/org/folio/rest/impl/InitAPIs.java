package org.folio.rest.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.folio.oaipmh.ResponseConverter;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.folio.rest.resource.interfaces.InitAPI;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * The class initializes system properties and checks if required configs are specified
 */
public class InitAPIs implements InitAPI {
  private final Logger logger = LoggerFactory.getLogger(InitAPIs.class);

  private static final String CONFIG_PATH = "config";
  private static final String[] CONFIGS = {"behavior.json","general.json","technical.json"};
  private ConfigurationHelper configurationHelper = ConfigurationHelper.getInstance();

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> resultHandler) {
    try {
      Properties systemProperties = System.getProperties();
      String configPath = systemProperties.getProperty("configPath", CONFIG_PATH);
      List<String> configsSet = Arrays.asList(CONFIGS);
      configsSet.forEach(configName -> {
        JsonObject jsonConfig = configurationHelper.getJsonConfigFromResources(configPath, configName);
        Map<String, String> configKeyValueMap = configurationHelper.getConfigKeyValueMapFromJsonEntryValueField(jsonConfig);
        configKeyValueMap.forEach((key, value) -> {
          Object existing = systemProperties.putIfAbsent(key, value);
          if (logger.isInfoEnabled()) {
            String message = (existing == null) ?
              String.format("The '%s' property was loaded from config file with its default value '%s'", key, value) :
              String.format("The '%s' system property has '%s' value. The default '%s' value from config file is ignored.", key, existing, value);
            logger.info(message);
          }
        });
      });

      // Initialize ResponseWriter and check if jaxb marshaller is ready to operate
      if (!ResponseConverter.getInstance()
        .isJaxbInitialized()) {
        throw new IllegalStateException("The jaxb marshaller failed initialization.");
      }
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
      logger.error("Unable to populate system properties", e);
    }
  }

}
