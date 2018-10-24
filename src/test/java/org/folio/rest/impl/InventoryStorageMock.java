package org.folio.rest.impl;

import static org.junit.Assert.fail;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.log4j.Logger;

public class InventoryStorageMock {

  private static final Logger logger = Logger.getLogger(InventoryStorageMock.class);
  static final String EXISTING_IDENTIFIER = "existing-identifier";
  static final String NON_EXISTING_IDENTIFIER = "non-existing-identifier";

  private final int port;
  private final Vertx vertx;

  InventoryStorageMock(int port) {
    this.port = port;
    this.vertx = Vertx.vertx();
  }

  private Router defineRoutes() {
    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    router.route(HttpMethod.GET, "/instance-storage/instances/" + EXISTING_IDENTIFIER)
      .handler(this::handleMarcJsonRetrieved);
    router.route(HttpMethod.GET, "/instance-storage/instances/" + NON_EXISTING_IDENTIFIER)
      .handler(this::handleMarcJsonNotRetrieved);
    return router;
  }

  public void start(TestContext context) {
    HttpServer server = vertx.createHttpServer();
    final Async async = context.async();
    server.requestHandler(defineRoutes()::accept).listen(port, result -> {
      if (result.failed()) {
        logger.warn(result.cause());
      }
      context.assertTrue(result.succeeded());
      async.complete();
    });
  }

  public void close() {
    vertx.close(res -> {
      if (res.failed()) {
        logger.error("Failed to shut down inventory storage mock", res.cause());
        fail(res.cause().getMessage());
      } else {
        logger.info("Successfully shut down inventory storage mock");
      }
    });
  }

  private void handleMarcJsonRetrieved(RoutingContext ctx) {
    ctx.response()
      .setStatusCode(200)
      .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
      .end();
    logger.info("Mock returns http status code: " + ctx.response().getStatusCode());
  }

  private void handleMarcJsonNotRetrieved(RoutingContext ctx) {
    ctx.response()
      .setStatusCode(404)
      .putHeader(HttpHeaders.CONTENT_TYPE, "text/plain")
      .end();
    logger.info("Mock returns http status code: " + ctx.response().getStatusCode());
  }
}
