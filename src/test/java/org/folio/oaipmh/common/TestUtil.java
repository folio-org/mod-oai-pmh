package org.folio.oaipmh.common;

import io.vertx.core.Vertx;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import lombok.extern.log4j.Log4j2;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.liquibase.SingleConnectionProvider;
import org.folio.rest.persist.PostgresClient;

@Log4j2
public class TestUtil {

  public static void prepareSchema(Vertx vertx, String tenantId) {
    log.info("Creating schema {}", PostgresClient.convertToPsqlStandard(tenantId));
    try (Connection connection = SingleConnectionProvider.getConnection(vertx, tenantId)) {
      connection.prepareStatement("create schema if not exists "
          + PostgresClient.convertToPsqlStandard(tenantId)).execute();
    } catch (Exception ex) {
      throw log.throwing(new IllegalStateException(ex));
    }
  }

  public static void initializeTestContainerDbSchema(Vertx vertx, String tenantId) {
    prepareSchema(vertx, tenantId);
    prepareTables(vertx, tenantId);
    LiquibaseUtil.initializeSchemaForTenant(vertx, tenantId);
  }

  public static void prepareTables(Vertx vertx, String tenantId) {
    log.info("Executing init_database_for_oaiPmhImplTest.sql", tenantId);
    try (Connection connection = SingleConnectionProvider.getConnection(vertx, tenantId)) {
      var sql = Files.readString(
          Path.of("src/test/resources/sql/init_database_for_oaiPmhImplTest.sql"));
      connection.prepareStatement(sql).execute();
    } catch (Exception ex) {
      throw log.throwing(new IllegalStateException(ex));
    }
  }

  public static void prepareUser(Vertx vertx, String tenantId, String username, String password) {
    log.info("Creating DB user {}", username);
    try (Connection connection = SingleConnectionProvider.getConnection(vertx, tenantId)) {
      connection.prepareStatement(("CREATE ROLE %s WITH LOGIN SUPERUSER INHERIT NOCREATEDB "
                                  + "NOCREATEROLE NOREPLICATION PASSWORD '%s';").formatted(
                                    username, password)).execute();
    } catch (Exception ex) {
      throw log.throwing(new IllegalStateException(ex));
    }
  }
}
