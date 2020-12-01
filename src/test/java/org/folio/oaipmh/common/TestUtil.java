package org.folio.oaipmh.common;

import java.sql.Connection;
import java.util.List;

import org.folio.liquibase.SingleConnectionProvider;
import org.folio.rest.tools.PomReader;
import org.jooq.Table;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;

public class TestUtil {

  public static void prepareDatabase(Vertx vertx, VertxTestContext testContext, String tenantId, List<Table> tables) {
    try (Connection connection = SingleConnectionProvider.getConnection(vertx, tenantId)) {
      connection.prepareStatement("create schema if not exists oaitest_mod_oai_pmh")
        .execute();
      connection.setSchema("oaitest_mod_oai_pmh");
    } catch (Exception ex) {
      testContext.failNow(ex);
    }
  }

  public static String getModuleId() {
    String moduleName = PomReader.INSTANCE.getModuleName()
      .replaceAll("_", "-");
    String moduleVersion = PomReader.INSTANCE.getVersion();
    return moduleName + "-" + moduleVersion;
  }

}
