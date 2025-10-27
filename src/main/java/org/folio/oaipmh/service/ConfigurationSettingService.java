package org.folio.oaipmh.service;

import io.vertx.core.Future;
import java.util.List;
import org.folio.rest.jooq.tables.pojos.ConfigurationSettings;

public interface ConfigurationSettingService {

  /**
   * Retrieves configuration value by config type and name.
   */
  Future<String> getConfigurationValueByKey(String configType, String name, String tenantId);

  /**
   * Retrieves configuration setting by name.
   */
  Future<ConfigurationSettings> getByConfigName(String configName, String tenantId);

  /**
   * Retrieves all configuration settings.
   */
  Future<List<ConfigurationSettings>> getAllConfigurations(String tenantId);

  /**
   * Saves or updates configuration setting.
   */
  Future<ConfigurationSettings> saveConfiguration(ConfigurationSettings configurationSetting, String tenantId);

  /**
   * Updates configuration value by name.
   */
  Future<ConfigurationSettings> updateConfigValue(String configName, String configValue, String tenantId);

  /**
   * Deletes configuration setting by name.
   */
  Future<Boolean> deleteByConfigName(String configName, String tenantId);

  /**
   * Checks if configuration exists by name.
   */
  Future<Boolean> existsByConfigName(String configName, String tenantId);
}
