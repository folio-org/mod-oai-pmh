package org.folio.oaipmh.dao;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.Map;

public interface ConfigurationDao {

  /**
   * Retrieves configuration by name from the database.
   *
   * @param configName the name of the configuration (e.g., "general", "behavioral", "technical")
   * @param tenantId   the tenant identifier
   * @return Future containing the configuration as JsonObject, or null if not found
   */
  Future<JsonObject> getConfigurationByName(String configName, String tenantId);

  /**
   * Retrieves all configurations from the database.
   *
   * @param tenantId the tenant identifier
   * @return Future containing a map of configuration name to JsonObject
   */
  Future<Map<String, JsonObject>> getAllConfigurations(String tenantId);

  /**
   * Updates configuration by name.
   *
   * @param configName  the name of the configuration
   * @param configValue the configuration value as JsonObject
   * @param tenantId    the tenant identifier
   * @return Future containing the updated configuration
   */
  Future<JsonObject> updateConfiguration(String configName, JsonObject configValue, String tenantId);

  /**
   * Creates a new configuration entry.
   *
   * @param configName  the name of the configuration
   * @param configValue the configuration value as JsonObject
   * @param tenantId    the tenant identifier
   * @return Future containing the created configuration
   */
  Future<JsonObject> createConfiguration(String configName, JsonObject configValue, String tenantId);
}
