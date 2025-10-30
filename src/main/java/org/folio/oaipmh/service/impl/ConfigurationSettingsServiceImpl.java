package org.folio.oaipmh.service.impl;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import org.folio.oaipmh.dao.ConfigurationSettingsDao;
import org.folio.oaipmh.service.ConfigurationSettingsService;
import org.springframework.stereotype.Service;

@Service
public class ConfigurationSettingsServiceImpl implements ConfigurationSettingsService {

  private final ConfigurationSettingsDao configurationSettingsDao;

  public ConfigurationSettingsServiceImpl(ConfigurationSettingsDao configurationSettingsDao) {
    this.configurationSettingsDao = configurationSettingsDao;
  }

  @Override
  public Future<JsonObject> getConfigurationSettingsById(String id, String tenantId) {
    return configurationSettingsDao.getConfigurationSettingsById(id, tenantId);
  }

  @Override
  public Future<JsonObject> getConfigurationSettingsByName(String configName, String tenantId) {
    return configurationSettingsDao.getConfigurationSettingsByName(configName, tenantId);
  }

  @Override
  public Future<JsonObject> updateConfigurationSettingsById(String id, JsonObject entry,
                                                            String tenantId, String userId) {
    return configurationSettingsDao.updateConfigurationSettingsById(id, entry, tenantId, userId);
  }

  @Override
  public Future<JsonObject> saveConfigurationSettings(JsonObject entry,
                                                      String tenantId, String userId) {
    return configurationSettingsDao.saveConfigurationSettings(entry, tenantId, userId);
  }

  @Override
  public Future<Boolean> deleteConfigurationSettingsById(String id, String tenantId) {
    return configurationSettingsDao.deleteConfigurationSettingsById(id, tenantId);
  }

  @Override
  public Future<JsonObject> getConfigurationSettingsList(int offset, int limit, String tenantId) {
    return configurationSettingsDao.getConfigurationSettingsList(offset, limit, tenantId);
  }

}
