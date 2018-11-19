package org.folio.rest.impl;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.junit5.VertxTestContext;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.junit.jupiter.api.Assertions.fail;

public class OkapiMockServer {


  private static final Logger logger = LoggerFactory.getLogger(OkapiMockServer.class);

  static final String EXISTING_IDENTIFIER = "existing-identifier";
  static final String NON_EXISTING_IDENTIFIER = "non-existing-identifier";
  static final String INVALID_IDENTIFIER = "non-existing-identifier";
  static final String ERROR_IDENTIFIER = "please-return-error";
  public static final String OAI_TEST_TENANT = "oaiTest";
  public static final String EXIST_CONFIG_TENANT = "test_diku";
  public static final String EXIST_CONFIG_TENANT_2 = "test_diku2";
  public static final String NON_EXIST_CONFIG_TENANT = "not_diku";

  // Dates
  static final String NO_RECORDS_DATE = "2011-11-11T11:11:11Z";
  static final String PARTITIONABLE_RECORDS_DATE = "2003-01-01T00:00:00Z";
  static final String ERROR_DATE = "2010-10-10T10:10:10Z";
  static final String RECORD_STORAGE_INTERNAL_SERVER_ERROR_DATE = "2001-01-01T01:01:01Z";
  static final String DATE_FOR_ONE_INSTANCE_BUT_WITHOT_RECORD = "2000-01-02T00:00:00Z";
  static final String DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOT_RECORD = "2000-01-02T03:04:05Z";
  static final String THREE_INSTANCES_DATE = "2018-12-12T12:12:12Z";

  // Instance UUID
  static final String NOT_FOUND_RECORD_INSTANCE_ID = "04489a01-f3cd-4f9e-9be4-d9c198703f45";
  private static final String INTERNAL_SERVER_ERROR_INSTANCE_ID = "6b4ae089-e1ee-431f-af83-e1133f8e3da0";

  // Paths to json files
  private static final String INSTANCES_0 = "/instance-storage/instances/instances_0.json";
  private static final String INSTANCES_1 = "/instance-storage/instances/instances_1.json";
  private static final String INSTANCES_1_NO_RECORD_SOURCE = "/instance-storage/instances/instances_1_withNoRecordSource.json";
  private static final String INSTANCES_2 = "/instance-storage/instances/instances_2.json";
  private static final String INSTANCES_3 = "/instance-storage/instances/instances_3.json";
  private static final String INSTANCES_4 = "/instance-storage/instances/instances_4.json";
  private static final String INSTANCES_10 = "/instance-storage/instances/instances_10.json";
  private static final String INSTANCES_11 = "/instance-storage/instances/instances_11.json";

  private static final String CONFIG_TEST = "/configurations.entries/config_test.json";
  private static final String CONFIG_EMPTY = "/configurations.entries/config_empty.json";
  private static final String CONFIG_OAI_TENANT = "/configurations.entries/config_oaiTenant.json";
  public static final String ERROR_TENANT = "error";


  private final int port;
  private final Vertx vertx;

  public OkapiMockServer(Vertx vertx, int port) {
    this.port = port;
    this.vertx = vertx;
  }

  private Router defineRoutes() {
    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    router.route(HttpMethod.GET, "/instance-storage/instances")
          .handler(this::handleInstancesInventoryStorageResponse);
    router.route(HttpMethod.GET, "/instance-storage/instances/:instanceId/source-record/marc-json")
          .handler(this::handleMarcJsonInventoryStorageResponse);
    router.route(HttpMethod.GET, "/configurations/entries")
          .handler(this::handleConfigurationModuleResponse);
    return router;
  }

  private void handleConfigurationModuleResponse(RoutingContext ctx) {
    switch (ctx.request().getHeader(OKAPI_TENANT)) {
      case EXIST_CONFIG_TENANT:
        successResponse(ctx, getJsonObjectFromFile(CONFIG_TEST));
        break;
      case EXIST_CONFIG_TENANT_2:
        successResponse(ctx, getJsonObjectFromFile(CONFIG_OAI_TENANT));
        break;
      case OAI_TEST_TENANT:
        successResponse(ctx, getJsonObjectFromFile(CONFIG_OAI_TENANT));
        break;
      case ERROR_TENANT:
        failureResponse(ctx, 500, "Internal Server Error");
        break;
      default:
        successResponse(ctx, getJsonObjectFromFile(CONFIG_EMPTY));
        break;
    }
  }

  private void handleMarcJsonInventoryStorageResponse(RoutingContext ctx) {
    String instanceId = ctx.request().getParam("instanceId");
    if (instanceId.equalsIgnoreCase(INTERNAL_SERVER_ERROR_INSTANCE_ID)) {
      failureResponse(ctx, 500, "Internal Server Error");
    } else if (instanceId.equalsIgnoreCase(NOT_FOUND_RECORD_INSTANCE_ID)) {
      failureResponse(ctx, 404, "Record not found");
    } else {
      String json = getJsonObjectFromFile(String.format("/instance-storage/instances/marc-%s.json", instanceId));
      if (isNotEmpty(json)) {
        successResponse(ctx, json);
      } else {
        successResponse(ctx, getJsonObjectFromFile("/instance-storage/instances/marc.json"));
      }
    }
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
      if (query.endsWith("id==" + EXISTING_IDENTIFIER)) {
        successResponse(ctx, getJsonObjectFromFile(INSTANCES_1));
      } else if (query.endsWith("id==" + NON_EXISTING_IDENTIFIER)) {
        successResponse(ctx, getJsonObjectFromFile(INSTANCES_0));
      } else if (query.contains(NO_RECORDS_DATE)) {
        successResponse(ctx, getJsonObjectFromFile(INSTANCES_0));
      } else if (query.endsWith("id==" + ERROR_IDENTIFIER)) {
        failureResponse(ctx, 500, "Internal Server Error");
      } else if (query.contains(ERROR_DATE)) {
        failureResponse(ctx, 500, "Internal Server Error");
      } else if (query.contains(PARTITIONABLE_RECORDS_DATE)) {
        successResponse(ctx, getJsonObjectFromFile(INSTANCES_11));
      } else if (query.contains(DATE_FOR_ONE_INSTANCE_BUT_WITHOT_RECORD) || query.contains(NOT_FOUND_RECORD_INSTANCE_ID)) {
        successResponse(ctx, getJsonObjectFromFile(INSTANCES_1_NO_RECORD_SOURCE));
      } else if (query.contains(RECORD_STORAGE_INTERNAL_SERVER_ERROR_DATE)) {
        successResponse(ctx, getJsonObjectFromFile(INSTANCES_2));
      } else if (query.contains(THREE_INSTANCES_DATE)) {
        successResponse(ctx, getJsonObjectFromFile(INSTANCES_3));
      } else if (query.contains(DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOT_RECORD)) {
        successResponse(ctx, getJsonObjectFromFile(INSTANCES_4));
      } else {
        successResponse(ctx, getJsonObjectFromFile(INSTANCES_10));
      }
      logger.info("Mock returns http status code: " + ctx.response().getStatusCode());
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private void successResponse(RoutingContext ctx, String body) {
    ctx.response()
       .setStatusCode(200)
       .putHeader(HttpHeaders.CONTENT_TYPE, "text/json")
       .end(body);
  }

  private void failureResponse(RoutingContext ctx, int code, String body) {
    ctx.response()
       .setStatusCode(code)
       .putHeader(HttpHeaders.CONTENT_TYPE, "text/plain")
       .end(body);
  }

  /**
   * Creates {@link JsonObject} from the json file
   * @param path path to json file to read
   * @return json as string from the json file
   */
  private String getJsonObjectFromFile(String path) {
    try {
      URL resource = OkapiMockServer.class.getResource(path);
      if (resource == null) {
        return null;
      }
      File file = new File(resource.getFile());
      byte[] encoded = Files.readAllBytes(Paths.get(file.getPath()));
      return new String(encoded, StandardCharsets.UTF_8);
    } catch (IOException e) {
      logger.error("Unexpected error", e);
      fail(e.getMessage());
    }
    return null;
  }
}
