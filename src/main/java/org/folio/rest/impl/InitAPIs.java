package org.folio.rest.impl;

import static org.folio.oaipmh.Constants.CONFIGS_SET;

import java.util.Map;
import java.util.Properties;

import org.folio.oaipmh.ResponseHelper;
import org.folio.oaipmh.helpers.resource.ResourceHelper;
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
  private ResourceHelper resourceHelper = new ResourceHelper();

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> resultHandler) {
    try {
      Properties systemProperties = System.getProperties();
      String configPath = systemProperties.getProperty("configPath", CONFIG_PATH);

      CONFIGS_SET.forEach(configName -> {
        JsonObject jsonConfig = resourceHelper.getJsonConfigFromResources(configPath, configName);
        Map<String, String> configKeyValueMap = resourceHelper.getConfigKeyValueMapFromJsonConfigEntry(jsonConfig);
        systemProperties.putAll(configKeyValueMap);
      });

      // Initialize ResponseWriter and check if jaxb marshaller is ready to operate
      if (!ResponseHelper.getInstance()
        .isJaxbInitialized()) {
        throw new IllegalStateException("The jaxb marshaller failed initialization.");
      }

      OaiPmhImpl.init(resultHandler);
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
      logger.error("Unable to populate system properties", e);
    }
  }

}
