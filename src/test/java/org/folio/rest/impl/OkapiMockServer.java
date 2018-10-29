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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.fail;

public class OkapiMockServer {

  private static final Logger logger = Logger.getLogger(OkapiMockServer.class);
  static final String EXISTING_IDENTIFIER = "existing-identifier";
  static final String NON_EXISTING_IDENTIFIER = "non-existing-identifier";
  static final String INVALID_IDENTIFIER = "non-existing-identifier";

  private final int port;
  private final Vertx vertx;

  OkapiMockServer(Vertx vertx, int port) {
    this.port = port;
    this.vertx = vertx;
  }

  private Router defineRoutes() {
    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    router.route(HttpMethod.GET, "/instance-storage/instances")
          .handler(this::handleInstancesInventoryStorageResponse);
    return router;
  }

  public void start(VertxTestContext context) {
    HttpServer server = vertx.createHttpServer();

    server.requestHandler(defineRoutes()::accept).listen(port, context.succeeding(result -> {
      logger.info("The server has started");
      context.completeNow();
    }));
  }

  private void handleInstancesInventoryStorageResponse(RoutingContext ctx) {
    String query = ctx.request().getParam("query");
    if (query != null)
    {
      String jsonFile = "/instance-storage/instances/instances_10.json";
      if (query.endsWith("id=" + EXISTING_IDENTIFIER)) {
        jsonFile = "/instance-storage/instances/instances_1.json";
      } else if (query.endsWith("id=" + NON_EXISTING_IDENTIFIER)) {
        jsonFile = "/instance-storage/instances/instances_0.json";
      }
      ctx.response()
         .setStatusCode(200)
         .putHeader(HttpHeaders.CONTENT_TYPE, "text/json")
         .end(getJsonObjectFromFile(jsonFile));
      logger.info("Mock returns http status code: " + ctx.response().getStatusCode());
    } else {
      throw new UnsupportedOperationException();
    }
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
