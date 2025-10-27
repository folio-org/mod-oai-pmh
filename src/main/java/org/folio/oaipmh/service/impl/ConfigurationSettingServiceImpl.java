package org.folio.oaipmh.service.impl;

import io.vertx.core.Future;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.oaipmh.dao.ConfigurationSettingDao;
import org.folio.oaipmh.service.ConfigurationSettingService;
import org.folio.rest.jooq.tables.pojos.ConfigurationSettings;
import org.springframework.stereotype.Service;

@Log4j2
@Service
@RequiredArgsConstructor
public class ConfigurationSettingServiceImpl implements ConfigurationSettingService {

  private ConfigurationSettingDao configurationSettingDao;

  @Override
  public Future<String> getConfigurationValueByKey(String configType, String name, String tenantId) {
    String configKey = buildConfigKey(configType, name);
    return configurationSettingDao.getByConfigName(configKey, tenantId)
      .map(config -> config != null ? config.getConfigValue() : null)
      .recover(throwable -> {
        log.warn("Failed to get configuration value for key: {}", configKey, throwable);
        return Future.succeededFuture(null);
      });
  }

  @Override
  public Future<ConfigurationSettings> getByConfigName(String configName, String tenantId) {
    return configurationSettingDao.getByConfigName(configName, tenantId);
  }

  @Override
  public Future<List<ConfigurationSettings>> getAllConfigurations(String tenantId) {
    return configurationSettingDao.getAllConfigurations(tenantId);
  }

  @Override
  public Future<ConfigurationSettings> saveConfiguration(ConfigurationSettings configurationSetting, String tenantId) {
    return configurationSettingDao.saveConfiguration(configurationSetting, tenantId);
  }

  @Override
  public Future<ConfigurationSettings> updateConfigValue(String configName, String configValue, String tenantId) {
    return configurationSettingDao.updateConfigValue(configName, configValue, tenantId);
  }

  @Override
  public Future<Boolean> deleteByConfigName(String configName, String tenantId) {
    return configurationSettingDao.deleteByConfigName(configName, tenantId);
  }

  @Override
  public Future<Boolean> existsByConfigName(String configName, String tenantId) {
    return configurationSettingDao.existsByConfigName(configName, tenantId);
  }

  /**
   * Builds configuration key from config type and name.
   * Example: buildConfigKey("general", "enableOaiService") -> "general.enableOaiService"
   */
  private String buildConfigKey(String configType, String name) {
    if (configType == null || configType.trim().isEmpty()) {
      return name;
    }
    return configType + "." + name;
  }
}
