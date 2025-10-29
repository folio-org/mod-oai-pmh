package org.folio.oaipmh.helpers;

import static java.lang.Boolean.parseBoolean;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.service.ConfigurationService;

public class RepositoryConfigurationUtil {

  private static final Logger logger = LogManager.getLogger(RepositoryConfigurationUtil.class);

  private static ConfigurationService configurationService;

  private RepositoryConfigurationUtil() {}

  /**
   * Set the configuration service instance.
   * This method should be called during application initialization.
   */
  public static void setConfigurationService(ConfigurationService service) {
    configurationService = service;
  }

  /**
   * Retrieve configuration for mod-oai-pmh from database and puts these
   * properties into context.
   *
   * @param okapiHeaders - okapi headers
   * @param requestId - unique identifier for current request
   * @return empty Future
   */
  public static Future<Void> loadConfiguration(Map<String, String> okapiHeaders,
      String requestId) {
    String tenant = okapiHeaders.get(OKAPI_TENANT);

    if (configurationService == null) {
      logger.error("ConfigurationService is not initialized");
      return Future.failedFuture(new IllegalStateException("ConfigurationService is not initialized"));
    }

    return configurationService.loadConfiguration(requestId, tenant)
        .<Void>map(config -> null)
        .onFailure(throwable -> {
          logger.error("Error occurred while loading configuration for {} tenant.", tenant, throwable);
        });
  }

  public static void replaceGeneratedConfigKeyWithExisted(String generatedRequestId,
      String existedRequestId) {
    if (configurationService != null) {
      configurationService.replaceGeneratedConfigKeyWithExisted(generatedRequestId, existedRequestId);
    }
  }

  /**
   * Gets value of the config either from shared config or from System properties as a fallback.
   *
   * @param requestId requestId
   * @param name      config key
   * @return value of the config either from shared config if present. Or from System properties
   *         as fallback.
   */
  public static String getProperty(String requestId, String name) {
    if (configurationService != null) {
      return configurationService.getProperty(requestId, name);
    }
    return System.getProperty(name);
  }

  public static boolean getBooleanProperty(String requestId, String name) {
    if (configurationService != null) {
      return configurationService.getBooleanProperty(requestId, name);
    }
    return parseBoolean(System.getProperty(name));
  }

  public static boolean isDeletedRecordsEnabled(String requestId) {
    if (configurationService != null) {
      return configurationService.isDeletedRecordsEnabled(requestId);
    }
    String defaultPropertyValue = System.getProperty(REPOSITORY_DELETED_RECORDS);
    return Boolean.parseBoolean(defaultPropertyValue);
  }

  public static void cleanConfigForRequestId(String requestId) {
    if (configurationService != null) {
      configurationService.cleanConfigForRequestId(requestId);
    }
  }

  public static JsonObject getConfig(String requestId) {
    if (configurationService != null) {
      return configurationService.getConfig(requestId);
    }
    return null;
  }
}
