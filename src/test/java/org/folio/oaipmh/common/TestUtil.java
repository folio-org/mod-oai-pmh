package org.folio.oaipmh.common;

import io.vertx.core.Vertx;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.liquibase.SingleConnectionProvider;
import org.folio.rest.persist.PostgresClient;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;

public class TestUtil {

  public static void prepareSchema(Vertx vertx, String tenantId) {
    try (Connection connection = SingleConnectionProvider.getConnection(vertx, tenantId)) {
      connection.prepareStatement("create schema if not exists " + PostgresClient.convertToPsqlStandard(tenantId))
        .execute();
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  public static void initializeTestContainerDbSchema(Vertx vertx, String tenantId) {
    prepareSchema(vertx, tenantId);
    prepareTables(vertx, tenantId);
    LiquibaseUtil.initializeSchemaForTenant(vertx, tenantId);
  }

  public static void prepareTables(Vertx vertx, String tenantId) {
    try (Connection connection = SingleConnectionProvider.getConnection(vertx, tenantId)) {
      var sql = Files.readString(Path.of("src/test/resources/sql/init_database_for_oaiPmhImplTest.sql"));
      connection.prepareStatement(sql)
        .execute();
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

}
