package org.folio.oaipmh.dao.impl;

import static org.folio.rest.jooq.Tables.CONFIGURATION_SETTINGS;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.folio.oaipmh.dao.ConfigurationDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.jooq.tables.records.ConfigurationSettingsRecord;
import org.jooq.JSONB;
import org.springframework.stereotype.Repository;

@Repository
public class ConfigurationDaoImpl implements ConfigurationDao {

  private final PostgresClientFactory postgresClientFactory;

  public ConfigurationDaoImpl(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<JsonObject> getConfigurationByName(String configName, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      return txQE.query(dslContext -> dslContext.selectFrom(CONFIGURATION_SETTINGS)
          .where(CONFIGURATION_SETTINGS.CONFIG_NAME.eq(configName))
          .limit(1))
        .map(queryResult -> {
          return queryResult.stream()
            .findFirst()
            .map(row -> {
              Row unwrappedRow = row.unwrap();
              String configValue = unwrappedRow.getString("config_value");
              return new JsonObject(configValue);
            })
            .orElse(null);
        });
    });
  }

  @Override
  public Future<Map<String, JsonObject>> getAllConfigurations(String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      return txQE.query(dslContext -> dslContext.selectFrom(CONFIGURATION_SETTINGS))
        .map(this::queryResultToConfigMap);
    });
  }

  @Override
  public Future<JsonObject> updateConfiguration(String configName,
                                                JsonObject configValue, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      JSONB jsonb = JSONB.valueOf(configValue.encode());
      return txQE.execute(dslContext -> dslContext.update(CONFIGURATION_SETTINGS)
          .set(CONFIGURATION_SETTINGS.CONFIG_VALUE, jsonb)
          .where(CONFIGURATION_SETTINGS.CONFIG_NAME.eq(configName)))
        .compose(updateCount -> {
          if (updateCount > 0) {
            return Future.succeededFuture(configValue);
          }
          return Future.failedFuture(
            new RuntimeException("Configuration with name '" + configName + "' not found"));
        });
    });
  }

  @Override
  public Future<JsonObject> createConfiguration(String configName,
                                                JsonObject configValue, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      ConfigurationSettingsRecord record = new ConfigurationSettingsRecord();
      record.setId(UUID.randomUUID());
      record.setConfigName(configName);
      record.setConfigValue(JSONB.valueOf(configValue.encode()));

      return txQE.executeAny(dslContext -> dslContext.insertInto(CONFIGURATION_SETTINGS)
          .set(record))
        .map(result -> configValue);
    });
  }

  private Map<String, JsonObject> queryResultToConfigMap(QueryResult queryResult) {
    Map<String, JsonObject> configMap = new HashMap<>();
    queryResult.stream().forEach(row -> {
      Row unwrappedRow = row.unwrap();
      String configName = unwrappedRow.getString("config_name");
      String configValue = unwrappedRow.getString("config_value");
      configMap.put(configName, new JsonObject(configValue));
    });
    return configMap;
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }
}
