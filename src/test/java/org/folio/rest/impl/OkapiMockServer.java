package org.folio.rest.impl;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.ILL_POLICIES_URI;
import static org.folio.oaipmh.Constants.INSTANCE_FORMATS_URI;
import static org.folio.oaipmh.Constants.LOCATION_URI;
import static org.folio.oaipmh.Constants.MATERIAL_TYPES_URI;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;

import static org.folio.oaipmh.Constants.RESOURCE_TYPES_URI;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
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
  private static final String JSON_FILE_ID = "e567b8e2-a45b-45f1-a85a-6b6312bdf4d8";
  private static final String ID_PARAM = "instanceId";

  // Dates
  static final String NO_RECORDS_DATE = "2011-11-11T11:11:11Z";
  private static final String NO_RECORDS_DATE_STORAGE = "2011-11-11T11:11:11";
  static final String PARTITIONABLE_RECORDS_DATE = "2003-01-01";
  static final String PARTITIONABLE_RECORDS_DATE_TIME = "2003-01-01T00:00:00Z";
  private static final String PARTITIONABLE_RECORDS_DATE_TIME_STORAGE = "2003-01-01T00:00:00";
  static final String ERROR_UNTIL_DATE = "2010-10-10T10:10:10Z";
  // 1 second should be added to storage until date time
  private static final String ERROR_UNTIL_DATE_STORAGE = "2010-10-10T10:10:11";
  static final String RECORD_STORAGE_INTERNAL_SERVER_ERROR_UNTIL_DATE = "2001-01-01T01:01:01Z";
  // 1 second should be added to storage until date time
  private static final String RECORD_STORAGE_INTERNAL_SERVER_ERROR_UNTIL_DATE_STORAGE = "2001-01-01T01:01:02";
  static final String DATE_FOR_ONE_INSTANCE_BUT_WITHOT_RECORD = "2000-01-02T00:00:00Z";
  private static final String DATE_FOR_ONE_INSTANCE_BUT_WITHOT_RECORD_STORAGE = "2000-01-02T00:00:00";
  static final String DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOT_RECORD = "2000-01-02T03:04:05Z";
  private static final String DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOT_RECORD_STORAGE = "2000-01-02T03:04:05";
  static final String DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOUT_EXTERNAL_IDS_HOLDER_FIELD = "2000-01-02T07:07:07Z";
  private static final String DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOUT__EXTERNAL_IDS_HOLDER_FIELD_STORAGE = "2000-01-02T07:07:07";
  static final String THREE_INSTANCES_DATE = "2018-12-12";
  static final String THREE_INSTANCES_DATE_WITH_ONE_MARK_DELETED_RECORD = "2017-11-11";
  static final String INVENTORY_INSTANCE_DATE = "2020-01-01";
  static final String THREE_INSTANCES_DATE_TIME = THREE_INSTANCES_DATE + "T12:12:12Z";
  static final String DATE_FOR_INSTANCES_10 = "2001-01-29";
  private static final String DATE_FOR_INSTANCES_10_STORAGE = "2001-01-29T00:00:00";

  // Instance UUID
  static final String NOT_FOUND_RECORD_INSTANCE_ID = "04489a01-f3cd-4f9e-9be4-d9c198703f45";

  // Paths to json files
  private static final String INSTANCES_0 = "/instances_0.json";
  private static final String INSTANCES_1 = "/instances_1.json";
  private static final String INSTANCES_1_NO_RECORD_SOURCE = "/instances_1_withNoRecordSource.json";

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

  private static final String SOURCE_STORAGE_RESULT_URI = "/source-storage/source-records";
  private static final String STREAMING_INVENTORY_INSTANCE_IDS_ENDPOINT = "/oai-pmh-view/enrichedInstances";
  private static final String STREAMING_INVENTORY_ITEMS_AND_HOLDINGS_ENDPOINT = "/oai-pmh-view/updatedInstanceIds";

  public static final String ERROR_TENANT = "error";

  private static final String LOCATION_JSON_PATH = "/filtering-conditions/locations.json";
  private static final String ILL_POLICIES_JSON_PATH = "/filtering-conditions/illPolicies.json";
  private static final String MATERIAL_TYPES_JSON_PATH = "/filtering-conditions/materialTypes.json";
  private static final String INSTANCE_TYPES_JSON_PATH = "/filtering-conditions/instanceTypes.json";
  private static final String INSTANCE_FORMATS_JSON_PATH = "/filtering-conditions/instanceFormats.json";

  private final int port;
  private final Vertx vertx;

  public OkapiMockServer(Vertx vertx, int port) {
    this.port = port;
    this.vertx = vertx;
  }

  private Router defineRoutes() {
    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());

    router.get(ILL_POLICIES_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(INSTANCE_FORMATS_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(RESOURCE_TYPES_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(LOCATION_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(MATERIAL_TYPES_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);

    router.get(SOURCE_STORAGE_RESULT_URI)
          .handler(this::handleRecordStorageResultResponse);

    router.get(SOURCE_STORAGE_RESULT_URI)
      .handler(this::handleSourceRecordStorageResponse);

    router.post(SOURCE_STORAGE_RESULT_URI)
      .handler(this::handleSourceRecordStorageResponse);

    router.get(CONFIGURATIONS_ENTRIES)
          .handler(this::handleConfigurationModuleResponse);

    router.post(STREAMING_INVENTORY_INSTANCE_IDS_ENDPOINT)
      .handler(this::handleStreamingInventoryItemsAndHoldingsResponse);
    //need to set a proper value to this constant cause i don't now what the second view endpoint is
    router.get(STREAMING_INVENTORY_ITEMS_AND_HOLDINGS_ENDPOINT)
      .handler(this::handleStreamingInventoryInstanceIdsResponse);
    return router;
  }

  private void handleStreamingInventoryInstanceIdsResponse(RoutingContext ctx) {
    String path = "inventory_view/instance_ids.json";
    Buffer buffer = Buffer.buffer();
    final String startDate = ctx.request().params().get("startDate");
    if (startDate != null && startDate.contains(INVENTORY_INSTANCE_DATE)) {
      buffer = vertx.fileSystem().readFileBlocking(path);
    }
    ctx.response().end(buffer);
  }

  private void handleStreamingInventoryItemsAndHoldingsResponse(RoutingContext ctx) {
    String path = "inventory_view/enriched_instances.json";
    Buffer buffer = vertx.fileSystem().readFileBlocking(path);
    ctx.response().end(buffer);
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

  private void handleSourceRecordStorageResponse(RoutingContext ctx){
    String json = getJsonObjectFromFile(String.format("/source-storage/records/%s", String.format("marc-%s.json", JSON_FILE_ID)));
    if (isNotEmpty(json)) {
      final String uri = ctx.request().absoluteURI();

      if (uri.contains(THREE_INSTANCES_DATE_WITH_ONE_MARK_DELETED_RECORD)) {
        json = getRecordJsonWithDeletedTrue(json);
      } else if (uri.contains(DATE_FOR_INSTANCES_10)) {
        json = getRecordJsonWithSuppressedTrue(json);
      } else if (uri.contains(THREE_INSTANCES_DATE)) {
        json = getRecordJsonWithSuppressedTrue(getRecordJsonWithDeletedTrue(json));
      } else if (uri.contains(NO_RECORDS_DATE)) {
        json = getJsonObjectFromFile("/instance-storage.instances" + "/instances_0.json");
      } else if (ctx.request().method() == HttpMethod.POST) {
        json = getJsonObjectFromFile("/source-storage/records/srs_instances.json");
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

  private void handleRecordStorageResultResponse(RoutingContext ctx) {
    String uri = ctx.request().absoluteURI();
    if (uri != null)
    {
      if (uri.contains(String.format("%s=%s", ID_PARAM, EXISTING_IDENTIFIER))) {
        successResponse(ctx, getJsonObjectFromFile(SOURCE_STORAGE_RESULT_URI + INSTANCES_1));
      } else if (uri.contains(String.format("%s=%s", ID_PARAM, NON_EXISTING_IDENTIFIER))) {
        successResponse(ctx, getJsonObjectFromFile(SOURCE_STORAGE_RESULT_URI + INSTANCES_0));
      } else if (uri.contains(NO_RECORDS_DATE_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFile(SOURCE_STORAGE_RESULT_URI + INSTANCES_0));
      } else if (uri.contains(String.format("%s=%s", ID_PARAM, ERROR_IDENTIFIER))) {
        failureResponse(ctx, 500, "Internal Server Error");
      } else if (uri.contains(ERROR_UNTIL_DATE_STORAGE)) {
        failureResponse(ctx, 500, "Internal Server Error");
      } else if (uri.contains(PARTITIONABLE_RECORDS_DATE_TIME_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFile(SOURCE_STORAGE_RESULT_URI + INSTANCES_11));
      } else if (uri.contains(DATE_FOR_ONE_INSTANCE_BUT_WITHOT_RECORD_STORAGE) || uri.contains(NOT_FOUND_RECORD_INSTANCE_ID)) {
        successResponse(ctx, getJsonObjectFromFile(SOURCE_STORAGE_RESULT_URI + INSTANCES_1_NO_RECORD_SOURCE));
      } else if (uri.contains(RECORD_STORAGE_INTERNAL_SERVER_ERROR_UNTIL_DATE_STORAGE)) {
        failureResponse(ctx, 500, "Internal Server Error");
      } else if (uri.contains(THREE_INSTANCES_DATE)) {
        successResponse(ctx, getJsonObjectFromFile(SOURCE_STORAGE_RESULT_URI + INSTANCES_3));
      } else if (uri.contains(DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOT_RECORD_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFile(SOURCE_STORAGE_RESULT_URI + INSTANCES_4));
      } else if (uri.contains(DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOUT__EXTERNAL_IDS_HOLDER_FIELD_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFile(SOURCE_STORAGE_RESULT_URI + INSTANCES_3_LAST_WITHOUT_EXTERNAL_IDS_HOLDER_FIELD));
      } else if (uri.contains(DATE_FOR_INSTANCES_10_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFile(SOURCE_STORAGE_RESULT_URI + INSTANCES_10_TOTAL_RECORDS_10));
      } else if (uri.contains(THREE_INSTANCES_DATE_WITH_ONE_MARK_DELETED_RECORD)) {
        String json = getJsonWithRecordMarkAsDeleted(getJsonObjectFromFile(SOURCE_STORAGE_RESULT_URI + INSTANCES_3));
        successResponse(ctx, json);
      } else {
        successResponse(ctx, getJsonObjectFromFile(SOURCE_STORAGE_RESULT_URI + INSTANCES_10_TOTAL_RECORDS_11));
      }
      logger.info("Mock returns http status code: " + ctx.response().getStatusCode());
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private void handleInventoryStorageFilteringConditionsResponse(RoutingContext ctx) {
    String uri = ctx.request().absoluteURI();
    if(uri.contains(ILL_POLICIES_URI)) {
      successResponse(ctx, getJsonObjectFromFile(ILL_POLICIES_JSON_PATH));
    } else if(uri.contains(INSTANCE_FORMATS_URI)) {
      successResponse(ctx, getJsonObjectFromFile(INSTANCE_FORMATS_JSON_PATH));
    } else if(uri.contains(RESOURCE_TYPES_URI)) {
      successResponse(ctx, getJsonObjectFromFile(INSTANCE_TYPES_JSON_PATH));
    } else if(uri.contains(LOCATION_URI)) {
      successResponse(ctx, getJsonObjectFromFile(LOCATION_JSON_PATH));
    } else if(uri.contains(MATERIAL_TYPES_URI)) {
      successResponse(ctx, getJsonObjectFromFile(MATERIAL_TYPES_JSON_PATH));
    } else {
      failureResponse(ctx, 400, "there is no mocked handler for request uri '{" + uri + "}'");
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
