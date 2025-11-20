package org.folio.oaipmh.helpers;

import static java.lang.Boolean.parseBoolean;
import static org.folio.oaipmh.Constants.CONFIGS;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;
import static org.openarchives.oai._2.DeletedRecordType.PERSISTENT;
import static org.openarchives.oai._2.DeletedRecordType.TRANSIENT;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.folio.oaipmh.service.ConfigurationSettingsService;
import org.openarchives.oai._2.DeletedRecordType;


public class RepositoryConfigurationUtil {

  private static final Logger logger = LogManager.getLogger(RepositoryConfigurationUtil.class);

  private static final String CONFIGURATION_ERROR = "Configuration service error for "
      + "%s tenant: %s";
  private static Map<String, JsonObject> configsMap = new HashMap<>();
  private static ConfigurationHelper configurationHelper = ConfigurationHelper.getInstance();
  private static ConfigurationSettingsService configurationSettingsService;

  private RepositoryConfigurationUtil() {
    // Private constructor to prevent instantiation
  }

  public static void setConfigurationSettingsService(ConfigurationSettingsService service) {
    configurationSettingsService = service;
  }


  /**
   * Retrieve configuration for mod-oai-pmh from own configuration service and puts these
   * properties into context.
   *
   * @param okapiHeaders - okapi headers
   * @param requestId - unique identifier for current request
   * @return empty CompletableFuture
   */
  public static Future<Void> loadConfiguration(Map<String, String> okapiHeaders,
      String requestId) {
    String tenant = okapiHeaders.get(OKAPI_TENANT);

    if (configurationSettingsService == null) {
      logger.error("ConfigurationSettingsService is not initialized for {} tenant.", tenant);
      return Future.failedFuture(
        new IllegalStateException("ConfigurationSettingsService is not initialized"));
    }

    try {
      // Fetch all OAIPMH configurations from own configuration service
      // Using offset=0 and limit=100 to get all configurations
      return configurationSettingsService.getConfigurationSettingsList(0, 100, null, tenant)
        .compose(response -> {
          try {
            JsonObject config = new JsonObject();
            
            // Debug: Log raw response
            logger.info("Raw configuration response for {} tenant: {}", tenant, 
                response != null ? response.encodePrettily() : "null");
            
            // Process the configuration settings
            if (response != null && response.containsKey(CONFIGS)) {
              response.getJsonArray(CONFIGS)
                  .stream()
                  .map(object -> (JsonObject) object)
                  .peek(entry -> logger.info("Processing config entry: {}", entry.encodePrettily()))
                  .map(configurationHelper::getConfigKeyValueMapFromJsonEntryValueField)
                  .peek(map -> logger.info("Mapped config: {}", map))
                  .forEach(configKeyValueMap ->
                      configKeyValueMap.forEach(config::put));
            }

            // Debug logging to verify configuration
            logger.info("Final configuration for {} tenant. Keys: {}", tenant, config.fieldNames());
            logger.info("repository.recordsSource = {}", 
                config.getString("repository.recordsSource"));
            logger.info("recordsSource (without prefix) = {}", config.getString("recordsSource"));
            logger.debug("Full configuration: {}", config.encodePrettily());

            configsMap.put(requestId, config);
            logger.info("Configuration loaded successfully for {} tenant.", tenant);
            return Future.<Void>succeededFuture();
          } catch (Exception e) {
            logger.error("Error occurred while processing configuration for {} tenant.",
                tenant, e);
            return Future.<Void>failedFuture(e);
          }
        })
        .recover(throwable -> {
          logger.error("Error loading configuration for {} tenant: {}",
              tenant, throwable.getMessage(), throwable);
          return Future.failedFuture(
              new IllegalStateException(
                  String.format(CONFIGURATION_ERROR, tenant, throwable.getMessage()),
                  throwable));
        });
    } catch (Exception e) {
      logger.error("Error initializing configuration service for {} tenant.", tenant, e);
      return Future.failedFuture(e);
    }
  }

  public static void replaceGeneratedConfigKeyWithExisted(String generatedRequestId,
      String existedRequestId) {
    configsMap.put(existedRequestId, configsMap.remove(generatedRequestId));
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
    JsonObject configs = getConfig(requestId);
    String defaultValue = System.getProperty(name);
    if (configs != null) {
      return configs.getString(name, defaultValue);
    }
    return defaultValue;
  }

  public static boolean getBooleanProperty(String requestId, String name) {
    JsonObject configs = getConfig(requestId);
    String defaultValue = System.getProperty(name);
    if (configs != null) {
      return parseBoolean(configs.getString(name, defaultValue));
    }
    return parseBoolean(defaultValue);
  }

  public static boolean isDeletedRecordsEnabled(String requestId) {
    String propertyName = getProperty(requestId, REPOSITORY_DELETED_RECORDS);
    try {
      DeletedRecordType deletedRecordType = DeletedRecordType.fromValue(propertyName);
      return Arrays.asList(PERSISTENT, TRANSIENT).contains(deletedRecordType);
    } catch (IllegalArgumentException ex) {
      String defaultPropertyValue = System.getProperty(REPOSITORY_DELETED_RECORDS);
      return Boolean.parseBoolean(defaultPropertyValue);
    }
  }

  public static void cleanConfigForRequestId(String requestId) {
    configsMap.remove(requestId);
  }

  public static JsonObject getConfig(String requestId) {
    return configsMap.get(requestId);
  }
}
