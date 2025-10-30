package org.folio.oaipmh.service.impl;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import javax.ws.rs.NotFoundException;
import org.folio.oaipmh.service.ConfigurationService;
import org.folio.oaipmh.service.ConfigurationSettingsService;
import org.springframework.stereotype.Service;

@Service
public class ConfigurationServiceImpl implements ConfigurationService {

  private final ConfigurationSettingsService configurationSettingsService;

  public ConfigurationServiceImpl(ConfigurationSettingsService configurationSettingsService) {
    this.configurationSettingsService = configurationSettingsService;
  }

  @Override
  public Future<JsonObject> getConfigByName(String configName, String tenantId) {
    return configurationSettingsService.getConfigurationSettingsByName(configName, tenantId)
        .map(config -> config.getJsonObject("configValue"))
        .recover(throwable -> {
          if (throwable instanceof NotFoundException) {
            // Return empty config if not found
            return Future.succeededFuture(new JsonObject());
          }
          return Future.failedFuture(throwable);
        });
  }

  @Override
  public Future<String> getStringValue(String configName, String propertyName, String tenantId) {
    return getConfigByName(configName, tenantId)
        .map(config -> config.getString(propertyName))
        .recover(throwable -> {
          // Return null if property not found
          return Future.succeededFuture(null);
        });
  }

  @Override
  public Future<Boolean> getBooleanValue(String configName, String propertyName, String tenantId) {
    return getConfigByName(configName, tenantId)
        .map(config -> config.getBoolean(propertyName))
        .recover(throwable -> {
          // Return null if property not found
          return Future.succeededFuture(null);
        });
  }

  @Override
  public Future<Integer> getIntegerValue(String configName, String propertyName, String tenantId) {
    return getConfigByName(configName, tenantId)
        .map(config -> config.getInteger(propertyName))
        .recover(throwable -> {
          // Return null if property not found
          return Future.succeededFuture(null);
        });
  }

  @Override
  public Future<JsonObject> updateConfig(String configName, JsonObject configValue, String tenantId, String userId) {
    return configurationSettingsService.getConfigurationSettingsByName(configName, tenantId)
        .compose(existingConfig -> {
          // Update existing configuration
          JsonObject updatedConfig = new JsonObject()
              .put("configName", configName)
              .put("configValue", configValue);
          
          return configurationSettingsService.updateConfigurationSettingsById(
              existingConfig.getString("id"), updatedConfig, tenantId, userId);
        })
        .recover(throwable -> {
          if (throwable instanceof NotFoundException) {
            // Create new configuration if not found
            JsonObject newConfig = new JsonObject()
                .put("configName", configName)
                .put("configValue", configValue);
            
            return configurationSettingsService.saveConfigurationSettings(newConfig, tenantId, userId);
          }
          return Future.failedFuture(throwable);
        });
  }

}
