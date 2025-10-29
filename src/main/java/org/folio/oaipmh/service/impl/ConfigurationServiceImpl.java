package org.folio.oaipmh.service.impl;

import static java.lang.Boolean.parseBoolean;
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
import org.folio.oaipmh.dao.ConfigurationDao;
import org.folio.oaipmh.service.ConfigurationService;
import org.openarchives.oai._2.DeletedRecordType;
import org.springframework.stereotype.Service;

@Service
public class ConfigurationServiceImpl implements ConfigurationService {

  private static final Logger logger = LogManager.getLogger(ConfigurationServiceImpl.class);

  private static Map<String, JsonObject> configsMap = new HashMap<>();

  private final ConfigurationDao configurationDao;

  public ConfigurationServiceImpl(ConfigurationDao configurationDao) {
    this.configurationDao = configurationDao;
  }

  @Override
  public Future<JsonObject> loadConfiguration(String requestId, String tenantId) {
    return configurationDao.getAllConfigurations(tenantId)
        .map(configMap -> {
          JsonObject mergedConfig = new JsonObject();

          // Merge all configuration values into a single JsonObject
          configMap.forEach((configName, configValue) -> {
            configValue.forEach(entry -> {
              mergedConfig.put(entry.getKey(), entry.getValue());
            });
          });

          configsMap.put(requestId, mergedConfig);
          return mergedConfig;
        })
        .onFailure(throwable -> {
          logger.error("Error occurred: {}", tenantId, throwable);
        });
  }

  @Override
  public void replaceGeneratedConfigKeyWithExisted(String generatedRequestId,
                                                   String existedRequestId) {
    configsMap.put(existedRequestId, configsMap.remove(generatedRequestId));
  }

  @Override
  public String getProperty(String requestId, String name) {
    JsonObject configs = getConfig(requestId);
    String defaultValue = System.getProperty(name);
    if (configs != null) {
      return configs.getString(name, defaultValue);
    }
    return defaultValue;
  }

  @Override
  public boolean getBooleanProperty(String requestId, String name) {
    JsonObject configs = getConfig(requestId);
    String defaultValue = System.getProperty(name);
    if (configs != null) {
      return parseBoolean(configs.getString(name, defaultValue));
    }
    return parseBoolean(defaultValue);
  }

  @Override
  public boolean isDeletedRecordsEnabled(String requestId) {
    String propertyName = getProperty(requestId, REPOSITORY_DELETED_RECORDS);
    try {
      DeletedRecordType deletedRecordType = DeletedRecordType.fromValue(propertyName);
      return Arrays.asList(PERSISTENT, TRANSIENT).contains(deletedRecordType);
    } catch (IllegalArgumentException ex) {
      String defaultPropertyValue = System.getProperty(REPOSITORY_DELETED_RECORDS);
      return Boolean.parseBoolean(defaultPropertyValue);
    }
  }

  @Override
  public void cleanConfigForRequestId(String requestId) {
    configsMap.remove(requestId);
  }

  @Override
  public JsonObject getConfig(String requestId) {
    return configsMap.get(requestId);
  }
}
