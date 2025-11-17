package org.folio.oaipmh.common;

import io.vertx.core.Vertx;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.UUID;
import lombok.extern.log4j.Log4j2;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.liquibase.SingleConnectionProvider;
import org.folio.rest.persist.PostgresClient;

@Log4j2
public class TestUtil {

  public static void prepareSchema(Vertx vertx, String tenantId) {
    log.info("Creating schema {}", PostgresClient.convertToPsqlStandard(tenantId));
    execute(vertx, tenantId, "create schema if not exists "
        + PostgresClient.convertToPsqlStandard(tenantId));
  }

  public static void initializeTestContainerDbSchema(Vertx vertx, String tenantId) {
    prepareSchema(vertx, tenantId);
    prepareExternalTables(vertx, tenantId);
    prepareInternalViews(vertx, tenantId);
    LiquibaseUtil.initializeSchemaForTenant(vertx, tenantId);
    insertDefaultConfigurationSettings(vertx, tenantId);
  }

  public static void prepareExternalTables(Vertx vertx, String tenantId) {
    log.info("Executing init_database_for_oaiPmhImplTest.sql", tenantId);
    executeFile(vertx, tenantId, "src/test/resources/sql/init_database_for_oaiPmhImplTest.sql");
  }

  public static void prepareInternalViews(Vertx vertx, String tenantId) {
    log.info("Executing init_database_views.sql", tenantId);
    executeFile(vertx, tenantId, "src/test/resources/sql/init_database_views.sql");
  }

  public static void prepareUser(Vertx vertx, String tenantId, String username, String password) {
    log.info("Creating DB user {}", username);
    execute(vertx, tenantId, ("CREATE ROLE %s WITH LOGIN SUPERUSER INHERIT NOCREATEDB "
                    + "NOCREATEROLE NOREPLICATION PASSWORD '%s';").formatted(username, password));
  }

  public static void insertDefaultConfigurationSettings(Vertx vertx, String tenantId) {
    log.info("Inserting default configuration settings for {}", tenantId);
    String schema = PostgresClient.convertToPsqlStandard(tenantId);
    String insertSql = String.format(
        "INSERT INTO %s.configuration_settings"
        + "(id, config_name, config_value) VALUES (?, ?, ?::jsonb)", schema);

    try (Connection connection = SingleConnectionProvider.getConnection(vertx, tenantId)) {
      // Insert the main configuration with baseUrl
      PreparedStatement ps = connection.prepareStatement(insertSql);
      ps.setObject(1, UUID.fromString("42b9ea94-ec9a-4904-9809-532ffc4573fe"));
      ps.setString(2, "repositoryBaseURL");
      ps.setString(3, "{\"repositoryName\":\"FOLIO_OAI_Repository_mock\",\"baseUrl\":\"http://test.folio.org/oai\",\"administratorEmail\":\"oai-pmh-admin1@folio.org,oai-pmh-admin2@folio.org\",\"enableOaiService\":true}");
      ps.execute();
      log.info("Inserted default configuration settings for {}", tenantId);
    } catch (Exception ex) {
      throw log.throwing(new IllegalStateException("Failed to insert configuration settings", ex));
    }
  }

  private static void executeFile(Vertx vertx, String tenantId, String filePath) {
    try {
      execute(vertx, tenantId, Files.readString(Path.of(filePath)));
    } catch (IOException ex) {
      throw log.throwing(new IllegalStateException(ex));
    }
  }

  private static void execute(Vertx vertx, String tenantId, String sql) {
    try (Connection connection = SingleConnectionProvider.getConnection(vertx, tenantId)) {
      connection.prepareStatement(sql).execute();
    } catch (Exception ex) {
      throw log.throwing(new IllegalStateException(ex));
    }
  }
}
