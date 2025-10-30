package org.folio.oaipmh.dao.impl;

import static org.folio.rest.jooq.Tables.CONFIGURATION_SETTINGS;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.dao.ConfigurationCrudDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.jaxrs.model.Configuration;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.Value;
import org.folio.rest.jooq.tables.records.ConfigurationSettingsRecord;
import org.jooq.JSONB;
import org.springframework.stereotype.Repository;

@Repository
public class ConfigurationCrudDaoImpl implements ConfigurationCrudDao {

  private final PostgresClientFactory postgresClientFactory;

  public ConfigurationCrudDaoImpl(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<List<Configuration>> getConfigurations(String tenantId, String configName, 
                                                      Boolean enabled, String query, 
                                                      int offset, int limit) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      return txQE.query(dslContext -> {
        if (StringUtils.isNotBlank(configName)) {
          return dslContext.selectFrom(CONFIGURATION_SETTINGS)
              .where(CONFIGURATION_SETTINGS.CONFIG_NAME.eq(configName))
              .orderBy(CONFIGURATION_SETTINGS.CONFIG_NAME)
              .offset(offset)
              .limit(limit);
        } else {
          return dslContext.selectFrom(CONFIGURATION_SETTINGS)
              .orderBy(CONFIGURATION_SETTINGS.CONFIG_NAME)
              .offset(offset)
              .limit(limit);
        }
      }).map(this::queryResultToConfigurationList);
    });
  }

  @Override
  public Future<Configuration> getConfigurationById(String configurationId, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      return txQE.query(dslContext -> dslContext.selectFrom(CONFIGURATION_SETTINGS)
          .where(CONFIGURATION_SETTINGS.ID.eq(UUID.fromString(configurationId)))
          .limit(1))
        .map(queryResult -> {
          return queryResult.stream()
            .findFirst()
            .map(this::mapRecordToConfiguration)
            .orElse(null);
        });
    });
  }

  @Override
  public Future<Configuration> getConfigurationByName(String configName, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      return txQE.query(dslContext -> dslContext.selectFrom(CONFIGURATION_SETTINGS)
          .where(CONFIGURATION_SETTINGS.CONFIG_NAME.eq(configName))
          .limit(1))
        .map(queryResult -> {
          return queryResult.stream()
            .findFirst()
            .map(this::mapRecordToConfiguration)
            .orElse(null);
        });
    });
  }

  @Override
  public Future<Configuration> createConfiguration(Configuration configuration, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      ConfigurationSettingsRecord record = new ConfigurationSettingsRecord();
      record.setId(UUID.fromString(configuration.getId()));
      record.setConfigName(configuration.getConfigName());
      record.setConfigValue(JSONB.valueOf(JsonObject.mapFrom(configuration.getValue()).encode()));

      return txQE.executeAny(dslContext -> dslContext.insertInto(CONFIGURATION_SETTINGS)
          .set(record))
        .map(result -> {
          // Set metadata for the created configuration
          Metadata metadata = new Metadata()
              .withCreatedDate(OffsetDateTime.now().toString());
          configuration.setMetadata(metadata);
          return configuration;
        });
    });
  }

  @Override
  public Future<Configuration> updateConfiguration(Configuration configuration, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      JSONB jsonb = JSONB.valueOf(JsonObject.mapFrom(configuration.getValue()).encode());
      return txQE.execute(dslContext -> dslContext.update(CONFIGURATION_SETTINGS)
          .set(CONFIGURATION_SETTINGS.CONFIG_VALUE, jsonb)
          .where(CONFIGURATION_SETTINGS.ID.eq(UUID.fromString(configuration.getId()))))
        .compose(updateCount -> {
          if (updateCount > 0) {
            // Update metadata
            Metadata metadata = configuration.getMetadata();
            if (metadata == null) {
              metadata = new Metadata();
              configuration.setMetadata(metadata);
            }
            metadata.setUpdatedDate(OffsetDateTime.now().toString());
            return Future.succeededFuture(configuration);
          }
          return Future.succeededFuture(null);
        });
    });
  }

  @Override
  public Future<Boolean> deleteConfiguration(String configurationId, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      return txQE.execute(dslContext -> dslContext.deleteFrom(CONFIGURATION_SETTINGS)
          .where(CONFIGURATION_SETTINGS.ID.eq(UUID.fromString(configurationId))))
        .map(deleteCount -> deleteCount > 0);
    });
  }

  private Configuration mapRecordToConfiguration(org.jooq.Record record) {
    Row row = record.unwrap();
    String id = row.getUUID("id").toString();
    String configName = row.getString("config_name");
    String configValue = row.getString("config_value");
    
    JsonObject valueJson = new JsonObject(configValue);
    
    Configuration configuration = new Configuration()
        .withId(id)
        .withConfigName(configName)
        .withValue(valueJson.getMap())
        .withEnabled(true) // Default enabled since legacy table doesn't have this field
        .withDescription("Configuration entry for " + configName);
    
    // Create basic metadata - in a real implementation you might want to store this in the database
    Metadata metadata = new Metadata()
        .withCreatedDate(OffsetDateTime.now().toString());
    configuration.setMetadata(metadata);
    
    return configuration;
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }
}
