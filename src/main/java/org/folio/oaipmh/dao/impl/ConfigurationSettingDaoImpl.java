package org.folio.oaipmh.dao.impl;

import static java.util.Objects.nonNull;
import static org.folio.rest.jooq.tables.ConfigurationSettings.CONFIGURATION_SETTINGS;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;

import java.util.List;
import java.util.Optional;
import javax.ws.rs.NotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.dao.ConfigurationSettingDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.ConfigurationSettings;
import org.folio.rest.jooq.tables.records.ConfigurationSettingsRecord;
import org.springframework.stereotype.Repository;

@Repository
public class ConfigurationSettingDaoImpl implements ConfigurationSettingDao {

  protected final Logger logger = LogManager.getLogger(getClass());

  private static final String CONFIG_NOT_FOUND_ERROR_MSG = "Configuration setting with name '%s' was not found";

  private final PostgresClientFactory postgresClientFactory;

  public ConfigurationSettingDaoImpl(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<ConfigurationSettings> getByConfigName(String configName, String tenantId) {
    return getQueryExecutorReader(tenantId).transaction(queryExecutor ->
        queryExecutor.findOneRow(dslContext ->
            dslContext.selectFrom(CONFIGURATION_SETTINGS)
                .where(CONFIGURATION_SETTINGS.CONFIG_NAME.eq(configName)))
        .map(this::toOptionalConfigurationSettings)
        .map(optionalConfig -> {
          if (optionalConfig.isPresent()) {
            return optionalConfig.get();
          }
          throw new NotFoundException(String.format(CONFIG_NOT_FOUND_ERROR_MSG, configName));
        }));
  }

  @Override
  public Future<List<ConfigurationSettings>> getAllConfigurations(String tenantId) {
    return getQueryExecutorReader(tenantId).transaction(queryExecutor ->
      queryExecutor.query(dslContext -> dslContext.selectFrom(CONFIGURATION_SETTINGS))
        .map(rows ->
          rows.stream()
            .map(RowMappers.getConfigurationSettingsMapper())
            .toList()
        )
    );
  }






  @Override
  public Future<ConfigurationSettings> saveConfiguration(ConfigurationSettings configurationSetting, String tenantId) {
    ConfigurationSettingsRecord record = toDatabaseRecord(configurationSetting);

    return getQueryExecutor(tenantId).transaction(queryExecutor ->
      queryExecutor.executeAny(dslContext ->
          dslContext.insertInto(CONFIGURATION_SETTINGS)
            .set(record)
            .onConflict(CONFIGURATION_SETTINGS.CONFIG_NAME)
            .doUpdate()
            .set(CONFIGURATION_SETTINGS.CONFIG_VALUE, configurationSetting.getConfigValue())
            .returning())
        .map(rows -> toOptionalConfigurationSettings(rows)) // ✅ lambda fixes type
        .map(optionalConfig -> optionalConfig.orElseThrow(
          () -> new RuntimeException("Failed to save configuration setting")
        ))
    );
  }

  @Override
  public Future<ConfigurationSettings> updateConfigValue(String configName, String configValue, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor ->
      queryExecutor.executeAny(dslContext ->
          dslContext.update(CONFIGURATION_SETTINGS)
            .set(CONFIGURATION_SETTINGS.CONFIG_VALUE, configValue)
            .where(CONFIGURATION_SETTINGS.CONFIG_NAME.eq(configName))
            .returning())
        .map(rows -> toOptionalConfigurationSettings(rows))
        .map(optionalConfig -> optionalConfig.orElseThrow(
          () -> new NotFoundException(String.format(CONFIG_NOT_FOUND_ERROR_MSG, configName))
        ))
    );
  }



  @Override
  public Future<Boolean> deleteByConfigName(String configName, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor ->
        queryExecutor.execute(dslContext ->
            dslContext.deleteFrom(CONFIGURATION_SETTINGS)
                .where(CONFIGURATION_SETTINGS.CONFIG_NAME.eq(configName)))
        .map(rowsAffected -> rowsAffected > 0));
  }

  @Override
  public Future<Boolean> existsByConfigName(String configName, String tenantId) {
    return getQueryExecutorReader(tenantId).transaction(queryExecutor ->
        queryExecutor.execute(dslContext ->
            dslContext.selectCount()
                .from(CONFIGURATION_SETTINGS)
                .where(CONFIGURATION_SETTINGS.CONFIG_NAME.eq(configName)))
        .map(count -> count > 0));
  }

  private Optional<ConfigurationSettings> toOptionalConfigurationSettings(List<Row> rows) {
    if (rows == null || rows.isEmpty()) return Optional.empty();
    Row firstRow = rows.get(0);
    ConfigurationSettings configurationSettings = RowMappers.getConfigurationSettingsMapper().apply(firstRow);
    return Optional.of(configurationSettings);
  }


  private ConfigurationSettingsRecord toDatabaseRecord(ConfigurationSettings configurationSettings) {
    ConfigurationSettingsRecord record = new ConfigurationSettingsRecord();
    record.setConfigName(configurationSettings.getConfigName());
    record.setConfigValue(configurationSettings.getConfigValue());
    return record;
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutorReader(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }
}
