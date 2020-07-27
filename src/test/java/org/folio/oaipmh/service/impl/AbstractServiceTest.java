package org.folio.oaipmh.service.impl;

import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.junit5.VertxTestContext;

public abstract class AbstractServiceTest {

  static final String TEST_TENANT_ID = "test_diku";
  static PostgresClientFactory postgresClientFactory;
  static Vertx vertx;

  @BeforeAll
  public static void setUpClass(VertxTestContext context) throws Exception {
    vertx = Vertx.vertx();

    PostgresClient.setIsEmbedded(true);

    PostgresClient.getInstance(vertx).startEmbeddedPostgres();

    postgresClientFactory = new PostgresClientFactory(vertx);

    int port = NetworkUtils.nextFreePort();
    String okapiUrl = "http://localhost:" + port;
    TenantClient tenantClient = new TenantClient(okapiUrl, TEST_TENANT_ID, "dummy-token");
    DeploymentOptions restVerticleDeploymentOptions = new DeploymentOptions()
      .setConfig(new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), restVerticleDeploymentOptions, deployResponse -> {
      try {
        tenantClient.postTenant(new TenantAttributes().withModuleTo("3.1.0"), postTenantResponse -> {
          context.completeNow();
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  @AfterAll
  public static void tearDownClass(VertxTestContext context) {
    PostgresClientFactory.closeAll();
    vertx.close(context.succeeding(res -> {
      PostgresClient.stopEmbeddedPostgres();
      context.completeNow();
    }));
  }
}
