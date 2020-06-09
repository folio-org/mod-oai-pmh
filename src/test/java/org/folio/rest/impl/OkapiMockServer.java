package org.folio.rest.impl;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.junit5.VertxTestContext;
import org.folio.oaipmh.helpers.storage.InventoryStorageHelper;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.helpers.storage.InventoryStorageHelper.MARC_JSON_RECORD_URI;
import static org.folio.oaipmh.helpers.storage.SourceRecordStorageHelper.SOURCE_STORAGE_RECORD_URI;
import static org.folio.oaipmh.helpers.storage.SourceRecordStorageHelper.SOURCE_STORAGE_RESULT_URI;
import static org.junit.jupiter.api.Assertions.fail;

public class OkapiMockServer {

  private static final Logger logger = LoggerFactory.getLogger(OkapiMockServer.class);

  static final String EXISTING_IDENTIFIER = "existing-identifier";
  static final String NON_EXISTING_IDENTIFIER = "non-existing-identifier";
  static final String INVALID_IDENTIFIER = "non-existing-identifier";
  static final String ERROR_IDENTIFIER = "please-return-error";
  static final String OAI_TEST_TENANT = "oaiTest";
  public static final String EXIST_CONFIG_TENANT = "test_diku";
  public static final String EXIST_CONFIG_TENANT_2 = "test_diku2";
  public static final String NON_EXIST_CONFIG_TENANT = "not_diku";
  public static final String JSON_FILE_ID = "e567b8e2-a45b-45f1-a85a-6b6312bdf4d8";

  // Dates
  static final String NO_RECORDS_DATE = "2011-11-11T11:11:11Z";
  private static final String NO_RECORDS_DATE_STORAGE = "2011-11-11T11:11:11.000Z";
  static final String PARTITIONABLE_RECORDS_DATE = "2003-01-01";
  static final String PARTITIONABLE_RECORDS_DATE_TIME = "2003-01-01T00:00:00Z";
  private static final String PARTITIONABLE_RECORDS_DATE_TIME_STORAGE = "2003-01-01T00:00:00.000Z";
  static final String ERROR_UNTIL_DATE = "2010-10-10T10:10:10Z";
  // 1 second should be added to storage until date time
  private static final String ERROR_UNTIL_DATE_STORAGE = "2010-10-10T10:10:11.000Z";
  static final String RECORD_STORAGE_INTERNAL_SERVER_ERROR_UNTIL_DATE = "2001-01-01T01:01:01Z";
  // 1 second should be added to storage until date time
  private static final String RECORD_STORAGE_INTERNAL_SERVER_ERROR_UNTIL_DATE_STORAGE = "2001-01-01T01:01:02.000Z";
  static final String DATE_FOR_ONE_INSTANCE_BUT_WITHOT_RECORD = "2000-01-02T00:00:00Z";
  private static final String DATE_FOR_ONE_INSTANCE_BUT_WITHOT_RECORD_STORAGE = "2000-01-02T00:00:00.000Z";
  static final String DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOT_RECORD = "2000-01-02T03:04:05Z";
  private static final String DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOT_RECORD_STORAGE = "2000-01-02T03:04:05.000Z";
  static final String DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOUT_EXTERNAL_IDS_HOLDER_FIELD = "2000-01-02T07:07:07Z";
  private static final String DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOUT__EXTERNAL_IDS_HOLDER_FIELD_STORAGE = "2000-01-02T07:07:07.000Z";
  static final String THREE_INSTANCES_DATE = "2018-12-12";
  static final String THREE_INSTANCES_DATE_WITH_ONE_MARK_DELETED_RECORD = "2017-11-11";
  static final String THREE_INSTANCES_DATE_TIME = THREE_INSTANCES_DATE + "T12:12:12Z";
  static final String DATE_FOR_INSTANCES_10 = "2001-01-29";
  private static final String DATE_FOR_INSTANCES_10_STORAGE = "2001-01-29T00:00:00.000Z";

  // Instance UUID
  static final String NOT_FOUND_RECORD_INSTANCE_ID = "04489a01-f3cd-4f9e-9be4-d9c198703f45";
  private static final String INTERNAL_SERVER_ERROR_INSTANCE_ID = "6b4ae089-e1ee-431f-af83-e1133f8e3da0";

  // Paths to json files
  private static final String INSTANCES_0 = "/instances_0.json";
  private static final String INSTANCES_1 = "/instances_1.json";
  private static final String INSTANCES_1_NO_RECORD_SOURCE = "/instances_1_withNoRecordSource.json";
  private static final String INSTANCES_2 = "/instances_2_lastWithStorageError500.json";
  private static final String INSTANCES_3 = "/instances_3.json";
  private static final String INSTANCES_4 = "/instances_4_lastWithNoRecordSource.json";
  private static final String INSTANCES_3_LAST_WITHOUT_EXTERNAL_IDS_HOLDER_FIELD = "/instances_3_lastWithoutExternalIdsHolderField.json";
  private static final String INSTANCES_10_TOTAL_RECORDS_10 = "/instances_10_totalRecords_10.json";
  private static final String INSTANCES_10_TOTAL_RECORDS_11 = "/instances_10_totalRecords_11.json";
  private static final String INSTANCES_11 = "/instances_11_totalRecords_100.json";

  private static final String CONFIG_TEST = "/configurations.entries/config_test.json";
  private static final String CONFIG_EMPTY = "/configurations.entries/config_empty.json";
  private static final String CONFIG_OAI_TENANT = "/configurations.entries/config_oaiTenant.json";
  private static final String CONFIGURATIONS_ENTRIES = "/configurations/entries";
  private static final String SOURCE_STORAGE_RECORD_PATH = "/source-storage/records";
  private static final String INVENTORY_PATH = "/oai-pmh-view/instances";

  private static final String INSTANCE_STORAGE_BY_ID_MARC_JSON = String.format(MARC_JSON_RECORD_URI, ":instanceId");
  private static final String SOURCE_STORAGE_RECORD = String.format(SOURCE_STORAGE_RECORD_URI, ":id");

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
    router.get(InventoryStorageHelper.INSTANCES_URI)
          .handler(this::handleInstancesInventoryStorageResponse);
    router.get(INSTANCE_STORAGE_BY_ID_MARC_JSON)
          .handler(this::handleMarcJsonInventoryStorageResponse);

    router.get(SOURCE_STORAGE_RESULT_URI)
          .handler(this::handleRecordStorageResultResponse);
    router.get(SOURCE_STORAGE_RECORD)
          .handler(this::handleSourceRecordStorageByIdResponse);
    router.get(SOURCE_STORAGE_RECORD_PATH)
      .handler(this::handleSourceRecordStorageResponse);

    router.get(CONFIGURATIONS_ENTRIES)
          .handler(this::handleConfigurationModuleResponse);

    router.get(INVENTORY_PATH)
      .handler(this::handleInventoryResponse);
    return router;
  }

  private void handleInventoryResponse(RoutingContext ctx) {
    if (ctx.request().absoluteURI().contains(THREE_INSTANCES_DATE)) {
      successResponse(ctx,  getJsonObjectFromFile("/instance-storage.instances" + "/instances_3.json"));
    } else if (ctx.request().absoluteURI().contains(NO_RECORDS_DATE)) {
      successResponse(ctx,  getJsonObjectFromFile("/instance-storage.instances" + "/instances_0.json"));
    } else if (ctx.request().absoluteURI().contains(DATE_FOR_INSTANCES_10_STORAGE)) {
      successResponse(ctx,  getJsonObjectFromFile("/instance-storage.instances" + "/instances_1.json"));
    } else {
      successResponse(ctx,  getJsonObjectFromFile("/instance-storage.instances" + "/instances_1.json"));
    }
    //successResponse(ctx,  getJsonObjectFromFile("/instance-storage.instances" + "/instances_1.json"));
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

  private void handleSourceRecordStorageByIdResponse(RoutingContext ctx) {
    String recordId = ctx.request().getParam("id");
      if (recordId.equalsIgnoreCase(INTERNAL_SERVER_ERROR_INSTANCE_ID)) {
        failureResponse(ctx, 500, "Internal Server Error");
      } else if (recordId.equalsIgnoreCase(NOT_FOUND_RECORD_INSTANCE_ID)) {
        failureResponse(ctx, 404, "Record not found");
      } else {
        String json = getJsonObjectFromFile(String.format(SOURCE_STORAGE_RECORD_URI, String.format("marc-%s.json", recordId)));
        if (isNotEmpty(json)) {
          successResponse(ctx, json);
        } else {
          fail("There is no mock response for recordId=" + recordId);
        }
      }
  }

  private void handleSourceRecordStorageResponse(RoutingContext ctx){
    String json = getJsonObjectFromFile(String.format(SOURCE_STORAGE_RECORD_URI, String.format("marc-%s.json", JSON_FILE_ID)));
    if (isNotEmpty(json)) {
      if (ctx.request().absoluteURI().contains(THREE_INSTANCES_DATE_WITH_ONE_MARK_DELETED_RECORD)) {
        json = getRecordJsonWithDeletedTrue(json);
      } else if (ctx.request().absoluteURI().contains(DATE_FOR_INSTANCES_10)) {
        json = getRecordJsonWithSuppressedTrue(json);
      } else if (ctx.request().absoluteURI().contains(THREE_INSTANCES_DATE)) {
        json = getRecordJsonWithSuppressedTrue(getRecordJsonWithDeletedTrue(json));
      } else if (ctx.request().absoluteURI().contains(NO_RECORDS_DATE)) {
        json = getJsonObjectFromFile("/instance-storage.instances" + "/instances_0.json");
      }
      successResponse(ctx, json);
    } else {
      fail("There is no mock response");
    }
  }

  public void start(VertxTestContext context) {
    HttpServer server = vertx.createHttpServer();

    server.requestHandler(defineRoutes()).listen(port, context.succeeding(result -> {
      logger.info("The server has started");
      context.completeNow();
    }));
  }

  private void handleInstancesInventoryStorageResponse(RoutingContext ctx) {
    handleResultResponse(ctx, InventoryStorageHelper.INSTANCES_URI);
  }

  private void handleRecordStorageResultResponse(RoutingContext ctx) {
    handleResultResponse(ctx, SOURCE_STORAGE_RESULT_URI);
  }

  private void handleResultResponse(RoutingContext ctx, String filePath) {
    String query = ctx.request().getParam("query");

    if (query != null)
    {
      if (query.endsWith(String.format("%s==%s", getIdParamName(filePath), EXISTING_IDENTIFIER))) {
        successResponse(ctx, getJsonObjectFromFile(filePath + INSTANCES_1));
      } else if (query.endsWith(String.format("%s==%s", getIdParamName(filePath), NON_EXISTING_IDENTIFIER))) {
        successResponse(ctx, getJsonObjectFromFile(filePath + INSTANCES_0));
      } else if (query.contains(NO_RECORDS_DATE_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFile(filePath + INSTANCES_0));
      } else if (query.endsWith(String.format("%s==%s", getIdParamName(filePath), ERROR_IDENTIFIER))) {
        failureResponse(ctx, 500, "Internal Server Error");
      } else if (query.contains(ERROR_UNTIL_DATE_STORAGE)) {
        failureResponse(ctx, 500, "Internal Server Error");
      } else if (query.contains(PARTITIONABLE_RECORDS_DATE_TIME_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFile(filePath + INSTANCES_11));
      } else if (query.contains(DATE_FOR_ONE_INSTANCE_BUT_WITHOT_RECORD_STORAGE) || query.contains(NOT_FOUND_RECORD_INSTANCE_ID)) {
        successResponse(ctx, getJsonObjectFromFile(filePath + INSTANCES_1_NO_RECORD_SOURCE));
      } else if (query.contains(RECORD_STORAGE_INTERNAL_SERVER_ERROR_UNTIL_DATE_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFile(filePath + INSTANCES_2));
      } else if (query.contains(THREE_INSTANCES_DATE)) {
        successResponse(ctx, getJsonObjectFromFile(filePath + INSTANCES_3));
      } else if (query.contains(DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOT_RECORD_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFile(filePath + INSTANCES_4));
      } else if (query.contains(DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOUT__EXTERNAL_IDS_HOLDER_FIELD_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFile(filePath + INSTANCES_3_LAST_WITHOUT_EXTERNAL_IDS_HOLDER_FIELD));
      } else if (query.contains(DATE_FOR_INSTANCES_10_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFile(filePath + INSTANCES_10_TOTAL_RECORDS_10));
      } else if (query.contains(THREE_INSTANCES_DATE_WITH_ONE_MARK_DELETED_RECORD)) {
        String json = getJsonWithRecordMarkAsDeleted(getJsonObjectFromFile(filePath + INSTANCES_3));
        successResponse(ctx, json);
      } else {
        successResponse(ctx, getJsonObjectFromFile(filePath + INSTANCES_10_TOTAL_RECORDS_11));
      }
      logger.info("Mock returns http status code: " + ctx.response().getStatusCode());
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private String getIdParamName(String filePath) {
    return SOURCE_STORAGE_RESULT_URI.equals(filePath) ? "externalIdsHolder.instanceId" : "id";
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
      logger.debug("Loading file " + path);
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

  private String getJsonWithRecordMarkAsDeleted(String json) {
    return json.replace("00778nam a2200217 c 4500","00778dam a2200217 c 4500");
  }

  private String getRecordJsonWithDeletedTrue(String json) {
    return json.replace("\"deleted\": false", "\"deleted\": true");
  }

  private String getRecordJsonWithSuppressedTrue(String json) {
    return json.replace("\"suppressDiscovery\": false", "\"suppressDiscovery\": true");
  }
}
