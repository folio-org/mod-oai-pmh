package org.folio.rest.impl;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.junit5.VertxTestContext;
import org.apache.log4j.Logger;
import org.hamcrest.CoreMatchers;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class OkapiMockServer {

  private static final Logger logger = Logger.getLogger(OkapiMockServer.class);
  static final String EXISTING_IDENTIFIER = "existing-identifier";
  static final String NON_EXISTING_IDENTIFIER = "non-existing-identifier";

  private final int port;
  private final Vertx vertx;

  OkapiMockServer(Vertx vertx, int port) {
    this.port = port;
    this.vertx = vertx;
  }

  private Router defineRoutes() {
    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    router.route(HttpMethod.GET, "/instance-storage/instances/" + EXISTING_IDENTIFIER)
      .handler(this::handleMarcJsonRetrieved);
    router.route(HttpMethod.GET, "/instance-storage/instances")
          .handler(this::tenInstancesInventoryStorageResponse);
    router.route(HttpMethod.GET, "/instance-storage/instances/" + NON_EXISTING_IDENTIFIER)
      .handler(this::handleMarcJsonNotRetrieved);
    return router;
  }

  public void start(VertxTestContext context) {
    HttpServer server = vertx.createHttpServer();

    server.requestHandler(defineRoutes()::accept).listen(port, result -> {
      if (result.failed()) {
        logger.warn(result.cause());
      }
      assertThat(result.succeeded(), CoreMatchers.is(true));
      context.completeNow();
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

  private void tenInstancesInventoryStorageResponse(RoutingContext ctx) {
    ctx.response()
      .setStatusCode(200)
      .putHeader(HttpHeaders.CONTENT_TYPE, "text/json")
      .end(getJsonObjectFromFile("/instance-storage/instances/instances_10.json"));
    logger.info("Mock returns http status code: " + ctx.response().getStatusCode());
  }

  /**
   * Creates {@link JsonObject} from the json file
   * @param path path to json file to read
   * @return json as string from the json file
   */
  private String getJsonObjectFromFile(String path) {
    try {
      File file = new File(OkapiMockServer.class.getResource(path).getFile());
      byte[] encoded = Files.readAllBytes(Paths.get(file.getPath()));
      return new String(encoded, StandardCharsets.UTF_8);
    } catch (IOException e) {
      logger.error("Unexpected error", e);
      fail(e.getMessage());
    }
    return null;
  }
}
