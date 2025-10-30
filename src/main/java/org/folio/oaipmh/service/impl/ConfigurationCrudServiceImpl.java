package org.folio.oaipmh.service.impl;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.dao.ConfigurationDao;
import org.folio.oaipmh.service.ConfigurationCrudService;
import org.folio.rest.jaxrs.model.Configuration;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ConfigurationCrudServiceImpl implements ConfigurationCrudService {

  @Autowired
  private ConfigurationDao configurationDao;

  @Override
  public Future<List<Configuration>> getConfigurations(String tenantId, String configName,
                                                      Boolean enabled, String query,
                                                      int offset, int limit) {
    if (StringUtils.isNotBlank(configName)) {
      // Get specific configuration by name
      return configurationDao.getConfigurationByName(configName, tenantId)
          .map(jsonObject -> {
            List<Configuration> configs = new ArrayList<>();
            if (jsonObject != null) {
              Configuration config = mapJsonToConfiguration(configName, jsonObject);
              configs.add(config);
            }
            return configs;
          });
    } else {
      // Get all configurations
      return configurationDao.getAllConfigurations(tenantId)
          .map(configMap -> {
            List<Configuration> configs = new ArrayList<>();
            configMap.forEach((name, jsonObject) -> {
              Configuration config = mapJsonToConfiguration(name, jsonObject);
              configs.add(config);
            });
            // Apply pagination
            int fromIndex = Math.min(offset, configs.size());
            int toIndex = Math.min(offset + limit, configs.size());
            return configs.subList(fromIndex, toIndex);
          });
    }
  }

  @Override
  public Future<Configuration> getConfigurationById(String configurationId, String tenantId) {
    // Since legacy table doesn't have UUID lookup, we'll need to scan all configurations
    return configurationDao.getAllConfigurations(tenantId)
        .map(configMap -> {
          for (var entry : configMap.entrySet()) {
            Configuration config = mapJsonToConfiguration(entry.getKey(), entry.getValue());
            if (configurationId.equals(config.getId())) {
              return config;
            }
          }
          return null;
        });
  }

  @Override
  public Future<Configuration> getConfigurationByName(String configName, String tenantId) {
    return configurationDao.getConfigurationByName(configName, tenantId)
        .map(jsonObject -> {
          if (jsonObject != null) {
            return mapJsonToConfiguration(configName, jsonObject);
          }
          return null;
        });
  }

  @Override
  public Future<Configuration> createConfiguration(Configuration configuration, String tenantId) {
    JsonObject configValue = JsonObject.mapFrom(configuration.getValue().getAdditionalProperties());
    
    return configurationDao.createConfiguration(configuration.getConfigName(), configValue, tenantId)
        .map(result -> {
          // Set metadata for created configuration
          Metadata metadata = new Metadata()
              .withCreatedDate(new Date());
          configuration.setMetadata(metadata);
          return configuration;
        });
  }

  @Override
  public Future<Configuration> updateConfiguration(Configuration configuration, String tenantId) {
    JsonObject configValue = JsonObject.mapFrom(configuration.getValue().getAdditionalProperties());
    
    return configurationDao.updateConfiguration(configuration.getConfigName(), configValue, tenantId)
        .map(result -> {
          if (result != null) {
            // Update metadata
            Metadata metadata = configuration.getMetadata();
            if (metadata == null) {
              metadata = new Metadata();
              configuration.setMetadata(metadata);
            }
            metadata.setUpdatedDate(new Date());
            return configuration;
          }
          return null;
        })
        .recover(throwable -> {
          if (throwable.getMessage().contains("not found")) {
            return Future.succeededFuture(null);
          }
          return Future.failedFuture(throwable);
        });
  }

  @Override
  public Future<Boolean> deleteConfiguration(String configurationId, String tenantId) {
    // First find the configuration by ID to get the config name
    return getConfigurationById(configurationId, tenantId)
        .compose(config -> {
          if (config != null) {
            // Delete by updating with null/empty - legacy table doesn't support real delete
            // This is a limitation of using the legacy configuration_settings table
            return Future.succeededFuture(false); // Indicate delete not supported
          }
          return Future.succeededFuture(false);
        });
  }

  private Configuration mapJsonToConfiguration(String configName, JsonObject jsonObject) {
    String id = UUID.nameUUIDFromBytes(configName.getBytes()).toString();
    
    Value value = new Value();
    jsonObject.getMap().forEach(value::setAdditionalProperty);
    
    Configuration configuration = new Configuration()
        .withId(id)
        .withConfigName(configName)
        .withValue(value)
        .withEnabled(true)
        .withDescription("Configuration entry for " + configName);
    
    // Create basic metadata
    Metadata metadata = new Metadata()
        .withCreatedDate(new Date());
    configuration.setMetadata(metadata);
    
    return configuration;
  }
}
