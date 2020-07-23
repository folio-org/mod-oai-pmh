package org.folio.oaipmh.service.impl;

import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public class AbstractServiceTest {

  static final String TENANT_ID = "diku";
  static PostgresClientFactory postgresClientFactory;
  static Vertx vertx;

  @BeforeClass
  public static void setUpClass(TestContext context) throws Exception {
    Async async = context.async();
    vertx = Vertx.vertx();

    PostgresClient.setIsEmbedded(true);

    PostgresClient.getInstance(vertx).startEmbeddedPostgres();

    postgresClientFactory = new PostgresClientFactory(vertx);

    int port = NetworkUtils.nextFreePort();
    String okapiUrl = "http://localhost:" + port;
    TenantClient tenantClient = new TenantClient(okapiUrl, "diku", "dummy-token");
    DeploymentOptions restVerticleDeploymentOptions = new DeploymentOptions()
      .setConfig(new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), restVerticleDeploymentOptions, deployResponse -> {
      try {
        tenantClient.postTenant(new TenantAttributes().withModuleTo("3.2.0"), postTenantResponse -> {
          async.complete();
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    PostgresClientFactory.closeAll();
    vertx.close(context.asyncAssertSuccess(res -> {
      PostgresClient.stopEmbeddedPostgres();
      async.complete();
    }));
  }
}
