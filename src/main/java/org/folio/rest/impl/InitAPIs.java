package org.folio.rest.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.folio.config.ApplicationConfig;
import org.folio.oaipmh.ResponseConverter;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.Logger;

/**
 * The class initializes system properties and checks if required configs are specified
 */
public class InitAPIs implements InitAPI {
  private final Logger logger = LogManager.getLogger(InitAPIs.class);

  private static final String CONFIGURATION_PATH = "configuration.path";
  private static final String CONFIGURATION_FILES = "configuration.files";
  private static final String DEFAULT_CONFIG_PATH = "config";
  private static final String DEFAULT_CONFIG_FILES = "behavior.json,general.json,technical.json";

  private ConfigurationHelper configurationHelper;

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> resultHandler) {
    try {
      SpringContextUtil.init(vertx, context, ApplicationConfig.class);
      SpringContextUtil.autowireDependencies(this, context);
      initSystemProperties(resultHandler);
      OaiPmhImpl.init();
      verifyJaxbInitialized();
      resultHandler.handle(Future.succeededFuture(true));
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }

  private void initSystemProperties(Handler<AsyncResult<Boolean>> handler) {
    try {
      Properties systemProperties = System.getProperties();
      String configPath = systemProperties.getProperty(CONFIGURATION_PATH, DEFAULT_CONFIG_PATH);
      String[] configFiles = systemProperties.getProperty(CONFIGURATION_FILES, DEFAULT_CONFIG_FILES).split(",");
      List<String> configsSet = Arrays.asList(configFiles);
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
    } catch (Exception e) {
      logger.error("Unable to populate system properties", e);
      handler.handle(Future.failedFuture(e));
    }
  }

  private void verifyJaxbInitialized() {
    if(!ResponseConverter.getInstance().isJaxbInitialized()) {
      throw new IllegalStateException("The jaxb marshaller failed initialization.");
    }
  }

  @Autowired
  public void setConfigurationHelper(ConfigurationHelper configurationHelper) {
    this.configurationHelper = configurationHelper;
  }

}
