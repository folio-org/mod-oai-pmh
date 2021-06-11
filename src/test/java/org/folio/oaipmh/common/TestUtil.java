package org.folio.oaipmh.common;

import java.sql.Connection;

import org.folio.liquibase.LiquibaseUtil;
import org.folio.liquibase.SingleConnectionProvider;
import org.folio.rest.persist.PostgresClient;

import io.vertx.core.Vertx;

public class TestUtil {

  public static void prepareSchema(Vertx vertx, String tenantId, String schemaName) {
    try (Connection connection = SingleConnectionProvider.getConnection(vertx, tenantId)) {
      connection.prepareStatement("create schema if not exists " + schemaName)
        .execute();
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  public static void initializeTestContainerDbSchema(Vertx vertx, String tenantId) {
    String schemaName = PostgresClient.convertToPsqlStandard(tenantId);
    prepareSchema(vertx, tenantId, schemaName);
    LiquibaseUtil.initializeSchemaForTenant(vertx, tenantId);
  }

}
