package org.folio.oaipmh.common;

import java.sql.Connection;

import org.folio.liquibase.LiquibaseUtil;
import org.folio.liquibase.SingleConnectionProvider;
import org.folio.rest.persist.PostgresClient;

import io.vertx.core.Vertx;

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
    LiquibaseUtil.initializeSchemaForTenant(vertx, tenantId);
  }

}
