package org.folio.oaipmh.dao.impl;

import static org.folio.rest.jooq.Tables.CONFIGURATION_SETTINGS;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgException;
import java.util.UUID;
import javax.ws.rs.NotFoundException;
import org.folio.oaipmh.dao.ConfigurationSettingsDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

@Repository
public class ConfigurationSettingsDaoImpl implements ConfigurationSettingsDao {

  private static final String ALREADY_EXISTS_ERROR_MSG =
        "Configuration " + "setting with id '%s' already exists";
  private static final String NOT_FOUND_ERROR_MSG =
        "Configuration setting with id '%s' was not found";
  private static final String CONFIG_NAME_NOT_FOUND_ERROR_MSG =
        "Configuration setting with config name '%s' was not found";

  private final PostgresClientFactory postgresClientFactory;

  public ConfigurationSettingsDaoImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<JsonObject> getConfigurationSettingsById(String id, String tenantId) {
    return getQueryExecutorReader(tenantId).transaction(txQE -> {
      return txQE.findOneRow(dslContext ->
          dslContext.selectFrom(CONFIGURATION_SETTINGS)
              .where(CONFIGURATION_SETTINGS.ID.eq(UUID.fromString(id))))
          .map(row -> {
            if (row == null) {
              throw new NotFoundException(String.format(NOT_FOUND_ERROR_MSG, id));
            }
            return mapRowToJsonObject(row);
          });
    });
  }

  @Override
  public Future<JsonObject> getConfigurationSettingsByName(String configName, String tenantId) {
    return getQueryExecutorReader(tenantId).transaction(txQE -> {
      return txQE.findOneRow(dslContext ->
          dslContext.selectFrom(CONFIGURATION_SETTINGS)
              .where(CONFIGURATION_SETTINGS.CONFIG_NAME.eq(configName)))
          .map(row -> {
            if (row == null) {
              throw new NotFoundException(
                String.format(CONFIG_NAME_NOT_FOUND_ERROR_MSG, configName));
            }
            return mapRowToJsonObject(row);
          });
    });
  }

  @Override
  public Future<JsonObject> updateConfigurationSettingsById(String id, JsonObject entry,
                                                            String tenantId, String userId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      return txQE.executeAny(dslContext ->
          dslContext.update(CONFIGURATION_SETTINGS)
              .set(CONFIGURATION_SETTINGS.CONFIG_NAME, entry.getString("configName"))
              .set(CONFIGURATION_SETTINGS.CONFIG_VALUE, DSL.cast(
                entry.getJsonObject("configValue").encode(), org.jooq.impl.SQLDataType.JSONB))
              .where(CONFIGURATION_SETTINGS.ID.eq(UUID.fromString(id)))
              .returning())
          .map(rows -> {
            if (rows.size() == 0) {
              throw new NotFoundException(String.format(NOT_FOUND_ERROR_MSG, id));
            }
            return mapRowToJsonObject(rows.iterator().next());
          });
    });
  }

  @Override
  public Future<JsonObject> saveConfigurationSettings(JsonObject entry,
                                                      String tenantId, String userId) {
    String id = entry.getString("id");
    if (id == null || id.isEmpty()) {
      id = UUID.randomUUID().toString();
      entry.put("id", id);
    }

    return getQueryExecutor(tenantId).transaction(txQE -> {
      return txQE.executeAny(dslContext ->
          dslContext.insertInto(CONFIGURATION_SETTINGS)
              .set(CONFIGURATION_SETTINGS.ID, UUID.fromString(entry.getString("id")))
              .set(CONFIGURATION_SETTINGS.CONFIG_NAME, entry.getString("configName"))
              .set(CONFIGURATION_SETTINGS.CONFIG_VALUE, DSL.cast(
                entry.getJsonObject("configValue").encode(), org.jooq.impl.SQLDataType.JSONB))
              .returning())
          .map(rows -> mapRowToJsonObject(rows.iterator().next()))
          .recover(throwable -> {
            if (throwable instanceof PgException) {
              PgException pgException = (PgException) throwable;
              if ("23505".equals(pgException.getSqlState())) { // unique violation
                throw new IllegalArgumentException(
                  String.format(ALREADY_EXISTS_ERROR_MSG, entry.getString("id")));
              }
            }
            throw new RuntimeException(throwable);
          });
    });
  }

  @Override
  public Future<Boolean> deleteConfigurationSettingsById(String id, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      return txQE.execute(dslContext ->
          dslContext.deleteFrom(CONFIGURATION_SETTINGS)
              .where(CONFIGURATION_SETTINGS.ID.eq(UUID.fromString(id))))
          .map(result -> {
            if (result == 0) {
              throw new NotFoundException(String.format(NOT_FOUND_ERROR_MSG, id));
            }
            return true;
          });
    });
  }

  @Override
  public Future<JsonObject> getConfigurationSettingsList(int offset, int limit, String tenantId) {
    return getQueryExecutorReader(tenantId).transaction(txQE -> {
      // Get the total count
      Future<Integer> countFuture = txQE.findOneRow(dslContext ->
          dslContext.selectCount()
              .from(CONFIGURATION_SETTINGS))
          .map(row -> row.getInteger(0));

      // Get the data
      Future<JsonObject> dataFuture = txQE.findManyRow(dslContext ->
          dslContext.selectFrom(CONFIGURATION_SETTINGS)
              .orderBy(CONFIGURATION_SETTINGS.CONFIG_NAME)
              .limit(limit)
              .offset(offset))
          .map(rows -> {
            JsonObject result = new JsonObject();
            JsonArray configArray = new JsonArray();
            rows.forEach(row -> configArray.add(mapRowToJsonObject(row)));
            result.put("configurationSettings", configArray);
            return result;
          });

      return Future.all(countFuture, dataFuture)
          .map(composite -> {
            JsonObject result = dataFuture.result();
            result.put("totalRecords", countFuture.result());
            return result;
          });
    });
  }

  private JsonObject mapRowToJsonObject(io.vertx.sqlclient.Row row) {
    JsonObject config = new JsonObject();
    config.put("id", row.getUUID("id").toString());
    config.put("configName", row.getString("config_name"));
    config.put("configValue", new JsonObject(row.getString("config_value")));
    return config;
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutorReader(String tenantId) {
    return postgresClientFactory.getQueryExecutorReader(tenantId);
  }
}
