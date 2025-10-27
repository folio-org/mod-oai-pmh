package org.folio.oaipmh.helpers;

import static java.lang.Boolean.parseBoolean;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;
import static org.openarchives.oai._2.DeletedRecordType.PERSISTENT;
import static org.openarchives.oai._2.DeletedRecordType.TRANSIENT;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.service.ConfigurationSettingService;
import org.folio.rest.jooq.tables.pojos.ConfigurationSettings;
import org.folio.spring.SpringContextUtil;
import org.openarchives.oai._2.DeletedRecordType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RepositoryConfigurationUtil {

  private static final Logger logger = LogManager.getLogger(RepositoryConfigurationUtil.class);

  // Configuration types
  private static final String GENERAL_CONFIG_TYPE = "general";
  private static final String BEHAVIOR_CONFIG_TYPE = "behavior";
  private static final String TECHNICAL_CONFIG_TYPE = "technical";

  // Legacy support for request-based configuration caching
  private static Map<String, JsonObject> configsMap = new HashMap<>();

  @Autowired
  private ConfigurationSettingService configurationService;

  // Static instance for legacy method support
  private static RepositoryConfigurationUtil instance;

  public RepositoryConfigurationUtil() {
    instance = this;
  }

  /**
   * Load configuration from database using ConfigurationSettingService.
   * This completely replaces mod-configuration.
   *
   * @param okapiHeaders - okapi headers
   * @param requestId - unique identifier for current request
   * @return empty Future
   */
  public static Future<Void> loadConfiguration(Map<String, String> okapiHeaders, String requestId) {
    String tenant = okapiHeaders.get(OKAPI_TENANT);

    // Initialize Spring context if needed
    if (instance == null) {
      SpringContextUtil.autowireDependencies(RepositoryConfigurationUtil.class,
        io.vertx.core.Vertx.currentContext());
    }

    if (instance != null && instance.configurationService != null) {
      return instance.loadConfigurationFromDatabase(tenant, requestId);
    } else {
      logger.warn("ConfigurationService not available, using empty configuration");
      configsMap.put(requestId, new JsonObject());
      return Future.succeededFuture(null);
    }
  }

  /**
   * Load configuration from database using ConfigurationSettingService.
   */
  private Future<Void> loadConfigurationFromDatabase(String tenant, String requestId) {
    return configurationService.getAllConfigurations(tenant)
      .map(configurations -> {
        JsonObject config = new JsonObject();

        // Convert configurations to legacy format for backward compatibility
        for (ConfigurationSettings setting : configurations) {
          try {
            // Parse JSON configuration values and flatten them
            if (setting.getConfigValue() != null) {
              JsonObject configJson = new JsonObject(setting.getConfigValue());
              configJson.fieldNames().forEach(key ->
                config.put(key, configJson.getString(key)));
            }
          } catch (Exception e) {
            logger.warn("Failed to parse configuration: {}", setting.getConfigName(), e);
            // Store as-is if JSON parsing fails
            config.put(setting.getConfigName(), setting.getConfigValue());
          }
        }

        configsMap.put(requestId, config);
        logger.info("Loaded {} configuration settings from database for tenant: {}",
          configurations.size(), tenant);
        return (Void) null;
      })
      .recover(throwable -> {
        logger.error("Error loading configuration from database for tenant: {} and request: {}",
          tenant, requestId, throwable);
        // Continue with empty config to maintain service availability
        configsMap.put(requestId, new JsonObject());
        return Future.succeededFuture(null);
      });
  }

  // New async methods for direct database access

  /**
   * Gets configuration property value asynchronously from database.
   */
  public Future<String> getProperty(String configType, String name, String tenantId) {
    if (configurationService != null) {
      return configurationService.getConfigurationValueByKey(configType, name, tenantId)
        .map(value -> Objects.nonNull(value) ? value : System.getProperty(name))
        .recover(throwable -> {
          logger.warn("Failed to get configuration value for {}.{}, falling back to system property",
            configType, name, throwable);
          return Future.succeededFuture(System.getProperty(name));
        });
    } else {
      return Future.succeededFuture(System.getProperty(name));
    }
  }

  /**
   * Gets boolean configuration property asynchronously from database.
   */
  public Future<Boolean> getBooleanProperty(String configType, String name, String tenantId) {
    return getProperty(configType, name, tenantId)
      .map(value -> parseBoolean(value));
  }

  /**
   * Checks if deleted records are enabled asynchronously.
   */
  public Future<Boolean> isDeletedRecordsEnabledAsync(String tenantId) {
    return getProperty(BEHAVIOR_CONFIG_TYPE, REPOSITORY_DELETED_RECORDS, tenantId)
      .map(propertyValue -> {
        try {
          DeletedRecordType deletedRecordType = DeletedRecordType.fromValue(propertyValue);
          return Arrays.asList(PERSISTENT, TRANSIENT).contains(deletedRecordType);
        } catch (IllegalArgumentException ex) {
          String defaultPropertyValue = System.getProperty(REPOSITORY_DELETED_RECORDS);
          return parseBoolean(defaultPropertyValue);
        }
      });
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
