package org.folio.oaipmh.service;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public interface ConfigurationService {

  /**
   * Load all configurations for a tenant and cache them by request ID.
   *
   * @param requestId    unique identifier for current request
   * @param tenantId     the tenant identifier
   * @return Future containing all configuration properties as a merged JsonObject
   */
  Future<JsonObject> loadConfiguration(String requestId, String tenantId);

  /**
   * Gets value of the config either from cached config or from System properties as a fallback.
   *
   * @param requestId requestId
   * @param name      config key
   * @return value of the config either from cached config if present. Or from System properties
   *         as fallback.
   */
  String getProperty(String requestId, String name);

  /**
   * Gets boolean value of the config either from cached config or from System properties as a fallback.
   *
   * @param requestId requestId
   * @param name      config key
   * @return boolean value of the config
   */
  boolean getBooleanProperty(String requestId, String name);

  /**
   * Check if deleted records are enabled based on configuration.
   *
   * @param requestId requestId
   * @return true if deleted records are enabled
   */
  boolean isDeletedRecordsEnabled(String requestId);

  /**
   * Clean configuration cache for a specific request ID.
   *
   * @param requestId requestId
   */
  void cleanConfigForRequestId(String requestId);

  /**
   * Replace generated config key with existed key in cache.
   *
   * @param generatedRequestId generated request ID
   * @param existedRequestId   existed request ID
   */
  void replaceGeneratedConfigKeyWithExisted(String generatedRequestId, String existedRequestId);

  /**
   * Get configuration object for a request.
   *
   * @param requestId requestId
   * @return configuration JsonObject
   */
  JsonObject getConfig(String requestId);
}
