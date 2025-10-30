package org.folio.oaipmh.dao;

import io.vertx.core.Future;
import java.util.List;
import org.folio.rest.jaxrs.model.Configuration;

/**
 * DAO interface for CRUD operations on Configuration entities.
 */
public interface ConfigurationCrudDao {

  /**
   * Get a list of configurations with optional filtering.
   *
   * @param tenantId   the tenant identifier
   * @param configName optional filter by configuration name
   * @param enabled    optional filter by enabled status
   * @param query      optional CQL query
   * @param offset     offset for pagination
   * @param limit      limit for pagination
   * @return Future containing list of configurations
   */
  Future<List<Configuration>> getConfigurations(String tenantId, String configName, 
                                               Boolean enabled, String query, 
                                               int offset, int limit);

  /**
   * Get configuration by ID.
   *
   * @param configurationId the configuration ID
   * @param tenantId        the tenant identifier
   * @return Future containing the configuration, or null if not found
   */
  Future<Configuration> getConfigurationById(String configurationId, String tenantId);

  /**
   * Get configuration by name.
   *
   * @param configName the configuration name
   * @param tenantId   the tenant identifier
   * @return Future containing the configuration, or null if not found
   */
  Future<Configuration> getConfigurationByName(String configName, String tenantId);

  /**
   * Create a new configuration.
   *
   * @param configuration the configuration to create
   * @param tenantId      the tenant identifier
   * @return Future containing the created configuration
   */
  Future<Configuration> createConfiguration(Configuration configuration, String tenantId);

  /**
   * Update an existing configuration.
   *
   * @param configuration the configuration to update
   * @param tenantId      the tenant identifier
   * @return Future containing the updated configuration, or null if not found
   */
  Future<Configuration> updateConfiguration(Configuration configuration, String tenantId);

  /**
   * Delete configuration by ID.
   *
   * @param configurationId the configuration ID
   * @param tenantId        the tenant identifier
   * @return Future containing true if deleted, false if not found
   */
  Future<Boolean> deleteConfiguration(String configurationId, String tenantId);
}
