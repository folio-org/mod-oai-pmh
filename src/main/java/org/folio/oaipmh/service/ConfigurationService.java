package org.folio.oaipmh.service;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

/**.
 * Service to provide configuration settings functionality, replacing mod-configuration dependency
 */
public interface ConfigurationService {

  /**.
   * Ge.t configuration value by config name

   * @param configName the name of the configuration
   * @param tenantId the tenant ID
   * @return Future containing the configuration value as JsonObject
   */
  Future<JsonObject> getConfigByName(String configName, String tenantId);

  /**.
   * Get a string value from configuration

   * @param configName the name of the configuration
   * @param propertyName the property name within the configuration
   * @param tenantId the tenant ID
   * @return Future containing the string value
   */
  Future<String> getStringValue(String configName, String propertyName, String tenantId);

  /**.
   * Get a boolean value from configuration

   * @param configName the name of the configuration
   * @param propertyName the property name within the configuration
   * @param tenantId the tenant ID
   * @return Future containing the boolean value
   */
  Future<Boolean> getBooleanValue(String configName, String propertyName, String tenantId);

  /**.
   * Get an integer value from configuration

   * @param configName the name of the configuration
   * @param propertyName the property name within the configuration
   * @param tenantId the tenant ID
   * @return Future containing the integer value
   */
  Future<Integer> getIntegerValue(String configName, String propertyName, String tenantId);

  /**.
   * Update configuration

   * @param configName the name of the configuration
   * @param configValue the new configuration value
   * @param tenantId the tenant ID
   * @param userId the user ID
   * @return Future containing the updated configuration
   */
  Future<JsonObject> updateConfig(String configName, JsonObject configValue,
                                  String tenantId, String userId);

}
