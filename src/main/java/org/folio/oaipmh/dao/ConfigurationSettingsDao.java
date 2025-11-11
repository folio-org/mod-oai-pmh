package org.folio.oaipmh.dao;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public interface ConfigurationSettingsDao {

  Future<JsonObject> getConfigurationSettingsById(String id, String tenantId);

  Future<JsonObject> getConfigurationSettingsByName(String configName, String tenantId);

  Future<JsonObject> updateConfigurationSettingsById(String id, JsonObject entry,
                                                     String tenantId, String userId);

  Future<JsonObject> saveConfigurationSettings(JsonObject entry, String tenantId, String userId);

  Future<Boolean> deleteConfigurationSettingsById(String id, String tenantId);

  Future<JsonObject> getConfigurationSettingsList(int offset, int limit, String name, String tenantId);

}
