package org.folio.rest.impl;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.folio.rest.RestVerticle;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class OaiPmhSetImplTest {
  private static Vertx vertx;
  private static int port;
  private static HttpClient httpClient;

  @BeforeClass
  public static void before() throws Exception {
    vertx = Vertx.vertx();
    httpClient = vertx.createHttpClient();
    port = NetworkUtils.nextFreePort();

    PostgresClient.setIsEmbedded(true);
    PostgresClient.setEmbeddedPort(NetworkUtils.nextFreePort());

    PostgresClient client = PostgresClient.getInstance(vertx);
    client.startEmbeddedPostgres();

    DeploymentOptions options = new DeploymentOptions();

    options.setConfig(new JsonObject().put("http.port", port));
    options.setWorker(true);

    startVerticle(options);

    prepareTenant("diku");
  }

  @AfterClass
  public static void after() throws Exception {
    CompletableFuture<String> undeploymentComplete = new CompletableFuture<>();

    vertx.close(res -> {
      if (res.succeeded()) {
        undeploymentComplete.complete(null);
      } else {
        undeploymentComplete.completeExceptionally(res.cause());
      }
    });

    undeploymentComplete.get(300000, TimeUnit.SECONDS);

    PostgresClient.stopEmbeddedPostgres();
  }

  private static void startVerticle(DeploymentOptions options)
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<String> deploymentComplete = new CompletableFuture<>();

    vertx.deployVerticle(RestVerticle.class.getName(), options, res -> {
      if (res.succeeded()) {
        deploymentComplete.complete(res.result());
      } else {
        deploymentComplete.completeExceptionally(res.cause());
      }
    });

    deploymentComplete.get(30, TimeUnit.SECONDS);
  }

  private static void prepareTenant(String tenantId) {
    CompletableFuture<Boolean> tenantPrepared = new CompletableFuture<>();

    try {
      JsonArray ar = new JsonArray();

      ar.add(new JsonObject().put("key", "loadReference").put("value", "true"));
      ar.add(new JsonObject().put("key", "loadSample").put("value", "true"));
      JsonObject jo = new JsonObject();
      jo.put("parameters", ar);
      jo.put("module_to", "mod-circulation-storage-1.0.0");

      httpClient.postAbs(okapi("/_/tenant"))
        .putHeader("X-Okapi-Tenant", tenantId)
        .putHeader("Content-Type", "application/json")
        .putHeader("Accept", "application/json, text/plain")
        .putHeader("X-Okapi-Url", okapi(""))
        .putHeader("X-Okapi-Url-to", okapi(""))
        .handler(res -> tenantPrepared.complete(res.statusCode() == 204))
        .end(jo.toBuffer());

      tenantPrepared.get(20, TimeUnit.SECONDS);

    } catch (Exception e) {
      assert false;
    }
  }

  private static String okapi(String uri) {
    return String.format("http://localhost:%s%s", port, uri);
  }

  @Test
  public void test() throws Exception {
    final CompletableFuture<String> responseFuture = new CompletableFuture<>();

    httpClient.getAbs(okapi("/oai-pmh/set/" + UUID.randomUUID()))
      .putHeader("X-Okapi-Tenant", "diku")
      .putHeader("X-Okapi-User-Id", UUID.randomUUID().toString())
      .putHeader("Accept", "application/json, text/plain")
      .putHeader("X-Okapi-Url", okapi(""))
      .putHeader("X-Okapi-Url-to", okapi(""))
      .handler(h -> h.bodyHandler(response -> responseFuture.complete(response.toString())))
      .end();

    System.out.println("#### " + responseFuture.get(10000, TimeUnit.SECONDS));
  }
}
