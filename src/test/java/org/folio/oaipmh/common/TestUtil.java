package org.folio.oaipmh.common;

import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.liquibase.SingleConnectionProvider;
import org.folio.rest.persist.PostgresClient;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;

import static java.lang.String.format;

public class TestUtil {

  private static final Logger logger = LogManager.getLogger(TestUtil.class);

  private static String moduleNameWithVersion;

  static {
    initTestVariables();
  }

  private static void initTestVariables() {
    MavenXpp3Reader reader = new MavenXpp3Reader();
    Model model;
    try {
      model = reader.read(new FileReader("pom.xml"));
      moduleNameWithVersion = model.getArtifactId() + "-" + model.getVersion();
    } catch (IOException | XmlPullParserException ex) {
      String errorMessage = format("Cannot initialize variable \"moduleName\". Reason: %s", ex.getMessage());
      logger.error(errorMessage, ex);
      throw new IllegalStateException(errorMessage, ex);
    }
  }

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

  public static String getModuleId() {
    return moduleNameWithVersion;
  }

}
