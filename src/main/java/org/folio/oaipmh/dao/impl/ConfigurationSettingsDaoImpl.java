package org.folio.oaipmh.dao.impl;

import static org.folio.rest.jooq.tables.ConfigurationSettings.CONFIGURATION_SETTINGS;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgException;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.NotFoundException;
import org.folio.oaipmh.dao.ConfigurationSettingsDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.exception.ConfigSettingException;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

@Repository
public class ConfigurationSettingsDaoImpl implements ConfigurationSettingsDao {

  private static final String ALREADY_EXISTS_ERROR_MSG =
      "Configuration setting with id '%s' already exists";
  private static final String NOT_FOUND_ERROR_MSG =
      "Configuration setting with id '%s' was not found";
  private static final String CONFIG_NAME_NOT_FOUND_ERROR_MSG =
      "Configuration setting with config name '%s' was not found";

  private static final DSLContext JOOQ = DSL.using(
      SQLDialect.POSTGRES,
      new Settings().withParamType(ParamType.INLINED));

  private final PostgresClientFactory postgresClientFactory;

  public ConfigurationSettingsDaoImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<JsonObject> getConfigurationSettingsById(String id, String tenantId) {
    UUID uuid = UUID.fromString(id);

    var query = JOOQ
        .select(
            CONFIGURATION_SETTINGS.ID,
            CONFIGURATION_SETTINGS.CONFIG_NAME,
            CONFIGURATION_SETTINGS.CONFIG_VALUE)
        .from(CONFIGURATION_SETTINGS)
        .where(CONFIGURATION_SETTINGS.ID.eq(uuid))
        .limit(1);

    return execute(reader(tenantId), query)
        .map(rows -> {
          if (!rows.iterator().hasNext()) {
            throw new NotFoundException(String.format(NOT_FOUND_ERROR_MSG, id));
          }
          return mapRowToJsonObject(rows.iterator().next());
        });
  }

  @Override
  public Future<JsonObject> getConfigurationSettingsByName(String configName, String tenantId) {

    var query = JOOQ
        .select(
            CONFIGURATION_SETTINGS.ID,
            CONFIGURATION_SETTINGS.CONFIG_NAME,
            CONFIGURATION_SETTINGS.CONFIG_VALUE)
        .from(CONFIGURATION_SETTINGS)
        .where(CONFIGURATION_SETTINGS.CONFIG_NAME.eq(configName))
        .limit(1);

    return execute(reader(tenantId), query)
        .map(rows -> {
          if (!rows.iterator().hasNext()) {
            throw new NotFoundException(
                String.format(CONFIG_NAME_NOT_FOUND_ERROR_MSG, configName));
          }
          return mapRowToJsonObject(rows.iterator().next());
        });
  }

  @Override
  public Future<JsonObject> updateConfigurationSettingsById(
      String id, JsonObject entry, String tenantId, String userId) {

    var uuid = UUID.fromString(id);
    var configName = entry.getString("configName");
    var configValueJson = entry.getJsonObject("configValue").encode();
    var jsonbValue = JSONB.valueOf(configValueJson);

    var query = JOOQ
        .update(CONFIGURATION_SETTINGS)
        .set(CONFIGURATION_SETTINGS.CONFIG_NAME, configName)
        .set(CONFIGURATION_SETTINGS.CONFIG_VALUE, jsonbValue)
        .where(CONFIGURATION_SETTINGS.ID.eq(uuid))
        .returning(
            CONFIGURATION_SETTINGS.ID,
            CONFIGURATION_SETTINGS.CONFIG_NAME,
            CONFIGURATION_SETTINGS.CONFIG_VALUE);

    return execute(writer(tenantId), query)
        .map(rows -> {
          if (!rows.iterator().hasNext()) {
            throw new NotFoundException(String.format(NOT_FOUND_ERROR_MSG, id));
          }
          return mapRowToJsonObject(rows.iterator().next());
        });
  }

  @Override
  public Future<JsonObject> saveConfigurationSettings(
      JsonObject entry, String tenantId, String userId) {

    var id = entry.getString("id");
    if (id == null || id.isEmpty()) {
      id = UUID.randomUUID().toString();
      entry.put("id", id);
    }

    final var idForErrorMessage = id;
    var uuid = UUID.fromString(id);
    var configName = entry.getString("configName");
    var configValueJson = entry.getJsonObject("configValue").encode();
    var jsonb = JSONB.valueOf(configValueJson);
    var query = JOOQ
        .insertInto(CONFIGURATION_SETTINGS)
        .set(CONFIGURATION_SETTINGS.ID, uuid)
        .set(CONFIGURATION_SETTINGS.CONFIG_NAME, configName)
        .set(CONFIGURATION_SETTINGS.CONFIG_VALUE, jsonb)
        .returning(
            CONFIGURATION_SETTINGS.ID,
            CONFIGURATION_SETTINGS.CONFIG_NAME,
            CONFIGURATION_SETTINGS.CONFIG_VALUE);

    return execute(writer(tenantId), query)
        .map(rows -> {
          RowIterator<Row> it = rows.iterator();
          if (it.hasNext()) {
            return mapRowToJsonObject(it.next());
          }
          throw new IllegalStateException(
              String.format(ALREADY_EXISTS_ERROR_MSG, idForErrorMessage));
        })
        .recover(throwable -> {
          if (throwable instanceof PgException pgException) {
            if ("23505".equals(pgException.getSqlState())
                && "configuration_settings_config_name_key"
                .equals(pgException.getConstraint())) {
              return Future.failedFuture(new ConfigSettingException(configName));
            }
          }
          return Future.failedFuture(throwable);
        });
  }

  @Override
  public Future<Boolean> deleteConfigurationSettingsById(String id, String tenantId) {

    var uuid = UUID.fromString(id);
    var query = JOOQ
        .deleteFrom(CONFIGURATION_SETTINGS)
        .where(CONFIGURATION_SETTINGS.ID.eq(uuid));

    return execute(writer(tenantId), query)
        .map(rows -> {
          if (rows.rowCount() == 0) {
            throw new NotFoundException(String.format(NOT_FOUND_ERROR_MSG, id));
          }
          return true;
        });
  }

  @Override
  public Future<JsonObject> getConfigurationSettingsList(
      int offset, int limit, String name, String tenantId) {

    Condition condition = null;
    if (name != null && !name.isEmpty()) {
      condition = CONFIGURATION_SETTINGS.CONFIG_NAME.eq(name);
    }

    var count = (condition == null)
        ? JOOQ.selectCount()
        .from(CONFIGURATION_SETTINGS)
        : JOOQ.selectCount()
            .from(CONFIGURATION_SETTINGS)
            .where(condition);

    var data = (condition == null)
        ? JOOQ
        .select(
            CONFIGURATION_SETTINGS.ID,
            CONFIGURATION_SETTINGS.CONFIG_NAME,
            CONFIGURATION_SETTINGS.CONFIG_VALUE)
        .from(CONFIGURATION_SETTINGS)
        .orderBy(CONFIGURATION_SETTINGS.CONFIG_NAME)
        .limit(limit)
        .offset(offset)
        : JOOQ
            .select(
                CONFIGURATION_SETTINGS.ID,
                CONFIGURATION_SETTINGS.CONFIG_NAME,
                CONFIGURATION_SETTINGS.CONFIG_VALUE)
            .from(CONFIGURATION_SETTINGS)
            .where(condition)
            .orderBy(CONFIGURATION_SETTINGS.CONFIG_NAME)
            .limit(limit)
            .offset(offset);

    var client = reader(tenantId);

    return execute(client, count)
        .compose(countRows -> {
          int total = countRows.iterator().next().getInteger(0);
          return execute(client, data)
              .map(dataRows -> {
                JsonArray configArray = new JsonArray();
                for (Row row : dataRows) {
                  configArray.add(mapRowToJsonObject(row));
                }

                JsonObject result = new JsonObject();
                result.put("configurationSettings", configArray);
                result.put("totalRecords", total);
                return result;
              });
        });
  }

  private JsonObject mapRowToJsonObject(Row row) {
    JsonObject config = new JsonObject();
    config.put("id", row.getUUID("id").toString());
    config.put("configName", row.getString("config_name"));

    Object rawValue = row.getValue("config_value");
    JsonObject configValueJson;

    switch (rawValue) {
      case JsonObject json -> configValueJson = json;
      case String jsonString -> configValueJson = new JsonObject(jsonString);
      case Map<?, ?> map -> {
        @SuppressWarnings("unchecked")
        Map<String, Object> casted = (Map<String, Object>) map;
        configValueJson = new JsonObject(casted);
      }
      case null -> configValueJson = new JsonObject();
      default -> throw new IllegalStateException(
          "Unexpected JSONB type for config_value: " + rawValue.getClass());
    }

    config.put("configValue", configValueJson);
    return config;
  }

  private Pool writer(String tenantId) {
    return postgresClientFactory.getPoolWriter(tenantId);
  }

  private Pool reader(String tenantId) {
    return postgresClientFactory.getPoolReader(tenantId);
  }

  private Future<RowSet<Row>> execute(Pool client, Query query) {
    var sql = query.getSQL(ParamType.INLINED);
    return client.query(sql).execute();
  }
}