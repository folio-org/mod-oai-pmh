package org.folio.rest.impl;

import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.ALTERNATIVE_TITLE_TYPES_URI;
import static org.folio.oaipmh.Constants.CALL_NUMBER_TYPES_URI;
import static org.folio.oaipmh.Constants.CAMPUSES_URI;
import static org.folio.oaipmh.Constants.CONTRIBUTOR_NAME_TYPES_URI;
import static org.folio.oaipmh.Constants.ELECTRONIC_ACCESS_RELATIONSHIPS_URI;
import static org.folio.oaipmh.Constants.HOLDINGS_NOTE_TYPES_URI;
import static org.folio.oaipmh.Constants.IDENTIFIER_TYPES_URI;
import static org.folio.oaipmh.Constants.ILL_POLICIES_URI;
import static org.folio.oaipmh.Constants.INSTANCE_FORMATS_URI;
import static org.folio.oaipmh.Constants.INSTITUTIONS_URI;
import static org.folio.oaipmh.Constants.ITEM_NOTE_TYPES_URI;
import static org.folio.oaipmh.Constants.LIBRARIES_URI;
import static org.folio.oaipmh.Constants.LOANTYPES_URI;
import static org.folio.oaipmh.Constants.LOCATION_URI;
import static org.folio.oaipmh.Constants.MATERIAL_TYPES_URI;
import static org.folio.oaipmh.Constants.MODES_OF_ISSUANCE_URI;
import static org.folio.oaipmh.Constants.NATURE_OF_CONTENT_TERMS_URI;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.RESOURCE_TYPES_URI;
import static org.folio.oaipmh.Constants.SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.junit5.VertxTestContext;

public class OkapiMockServer {

  private static final Logger logger = LogManager.getLogger(OkapiMockServer.class);

  public static final String TEST_USER_ID = "30fde4be-2d1a-4546-8d6c-b468caca2720";

  static final String EXISTING_IDENTIFIER = "existing-identifier";
  static final String RECORD_IDENTIFIER_MARC21_WITH_HOLDINGS = "00000000-0000-4a89-a2f9-78ce3145e4fc";
  static final String RECORD_IDENTIFIER_INSTANCE_NOT_FOUND = "fb3e23e5-eb7f-4b8b-b531-40e74ec9c6e9";
  static final String NON_EXISTING_IDENTIFIER = "non-existing-identifier";
  static final String INVALID_IDENTIFIER = "non-existing-identifier";
  static final String ERROR_IDENTIFIER = "please-return-error";
  public static final String OAI_TEST_TENANT = "oaiTest";
  public static final String EXIST_CONFIG_TENANT = "test_diku";
  public static final String EXIST_CONFIG_TENANT_2 = "test_diku2";
  public static final String NON_EXIST_CONFIG_TENANT = "not_diku";
  public static final String INVALID_JSON_TENANT = "invalidJsonTenant";
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
  static final String SRS_RECORD_WITH_INVALID_JSON_STRUCTURE = "2020-02-02";
  static final String TWO_RECORDS_WITH_ONE_INCONVERTIBLE_TO_XML = "2020-03-03";
  private static final String RECORD_STORAGE_INTERNAL_SERVER_ERROR_UNTIL_DATE_STORAGE = "2001-01-01T01:01:02";
  static final String DATE_FOR_ONE_INSTANCE_BUT_WITHOT_RECORD = "2000-01-02T00:00:00Z";
  private static final String DATE_FOR_ONE_INSTANCE_BUT_WITHOT_RECORD_STORAGE = "2000-01-02T00:00:00";
  static final String DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOT_RECORD = "2000-01-02T03:04:05Z";
  private static final String DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOT_RECORD_STORAGE = "2000-01-02T03:04:05";
  static final String DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOUT_EXTERNAL_IDS_HOLDER_FIELD = "2000-01-02T07:07:07Z";
  private static final String DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOUT__EXTERNAL_IDS_HOLDER_FIELD_STORAGE = "2000-01-02T07:07:07";
  static final String THREE_INSTANCES_DATE = "2018-12-12";
  static final String TEN_INSTANCES_WITH_HOLDINGS_DATE = "2018-07-07";
  static final String THREE_INSTANCES_DATE_WITH_ONE_MARK_DELETED_RECORD = "2017-11-11";
  static final String THREE_INSTANCES_DATE_WITH_ONE_MARK_DELETED_RECORD_LIST_RECORDS = "2017-11-13";
  static final String THREE_INSTANCES_DATE_TIME = THREE_INSTANCES_DATE + "T12:12:12Z";
  static final String DATE_FOR_INSTANCES_10_PARTIALLY = "2002-01-29";
  static final String DATE_FOR_INSTANCES_10 = "2001-01-29";
  static final String DATE_FOR_INSTANCES_FOLIO_AND_MARC_10 = "2001-03-29";
  static final String INVENTORY_27_INSTANCES_IDS_DATE = "2020-01-01";
  static final String DATE_INVENTORY_STORAGE_ERROR_RESPONSE = "1488-01-02";
  static final String DATE_SRS_ERROR_RESPONSE = "1388-01-01";
  static final String DATE_SRS_500_ERROR_RESPONSE = "1388-02-02";
  static final String DATE_SRS_IDLE_TIMEOUT_ERROR_RESPONSE = "1388-03-03";
  static final String DATE_INVENTORY_10_INSTANCE_IDS = "1499-01-01";
  static final String EMPTY_INSTANCES_IDS_DATE = "1444-01-01";
  static final String INVENTORY_60_INSTANCE_IDS_DATE = "2002-02-04";
  static final String INSTANCE_WITHOUT_SRS_RECORD_DATE = "2002-02-03";
  static final String DATE_ERROR_FROM_ENRICHED_INSTANCES_VIEW = "1433-01-03";
  static final String SRS_RECORD_WITH_OLD_METADATA_DATE = "1999-01-01";
  static final String SRS_RECORD_WITH_NEW_METADATA_DATE = "1999-02-02";
  static final String DEFAULT_RECORD_DATE = "2020-03-31";
  static final String SRS_RECORDS_WITH_CYRILLIC_DATA_DATE = "2002-02-02";
  static final String SUPPRESSED_RECORDS_DATE = "2020-03-30";
  static final String NO_ITEMS_DATE = "2020-01-29";
  static final String GET_INSTANCES_FORBIDDEN_RESPONSE_DATE = "2020-01-10";
  static final String ENRICH_INSTANCES_FORBIDDEN_RESPONSE_DATE = "2020-01-11";
  static final String GET_INSTANCES_IDS_500_ERROR_RETURNED_FROM_STORAGE_DATE = "2020-01-12";
  static final String GET_ENRICHED_INSTANCES_500_ERROR_RETURNED_FROM_STORAGE_DATE = "2020-01-12";

  public static final String INVALID_INSTANCE_IDS_JSON_DATE = "2011-11-22";
  public static final String INSTANCE_ID_WITH_INVALID_ENRICHED_INSTANCE_JSON_DATE = "2012-11-22";

  private static final String OLD_METADATA_DATE_FORMAT = "2020-12-02T11:24:07.230+0000";
  private static final String NEW_METADATA_DATE_FORMAT = "2020-09-03T07:47:40.097";
  // Instance UUID
  private static final String NOT_FOUND_RECORD_INSTANCE_ID = "04489a01-f3cd-4f9e-9be4-d9c198703f45";
  private static final String INVALID_SRS_RECORD_INSTANCE_ID = "68aaeff5-6c78-4498-9cdc-66cdc0f834b2";
  private static final String TWO_RECORDS_WITH_ONE_INCONVERTIBLE_TO_XML_INSTANCE_ID = "7b6d9a58-ab67-414b-a33f-7db11ea16178";

  private static final String INSTANCE_ID_TO_MAKE_SRS_FAIL = "12345678-0000-4000-a000-000000000000";
  private static final String INSTANCE_ID_TO_MAKE_SRS_FAIL_WITH_500 = "927ee35f-700c-4fdd-a7e9-b560861d6900";
  private static final String INSTANCE_ID_TO_MAKE_SRS_FAIL_BY_TIMEOUT = "d93c7b03-6343-4956-bfbc-2981b3741830";
  private static final String INSTANCE_ID_TO_FAIL_ENRICHED_INSTANCES_REQUEST = "22200000-0000-4000-a000-000000000000";
  private static final String INSTANCE_ID_RELATED_ENRICHED_INSTANCE_HAS_INVALID_JSON = "210f0f47-e0f8-4d01-83f3-2b51cf369699";
  private static final String INSTANCE_ID_RELATED_ENRICHED_INSTANCE_HAS_NO_ITEMS = "3a6a47ac-597d-4abe-916d-e35c72340000";

  // Paths to json files
  private static final String SRS_RECORD_WITH_NON_EXISTING_INSTANCE_JSON = "/srs_record_with_non_existing_instance.json";
  private static final String INSTANCES_0 = "/instances_0.json";
  private static final String INSTANCES_1 = "/instances_1.json";
  private static final String INSTANCES_1_NO_RECORD_SOURCE = "/instances_1_withNoRecordSource.json";

  private static final String INSTANCES_3 = "/instances_3.json";
  private static final String INSTANCES_3_WITH_DELETED = "/instances_3_with_deleted.json";
  private static final String INSTANCES_4 = "/instances_4_lastWithNoRecordSource.json";
  private static final String INSTANCES_3_LAST_WITHOUT_EXTERNAL_IDS_HOLDER_FIELD = "/instances_3_lastWithoutExternalIdsHolderField.json";
  private static final String INSTANCES_10_TOTAL_RECORDS_10 = "/instances_10_totalRecords_10.json";
  private static final String INSTANCES_WITH_SOURCE_FOLIO = "/instances_with_source_folio.json";
  private static final String INSTANCE_WITH_SOURCE_FOLIO = "/instance_with_source_folio.json";
  private static final String INSTANCES_10_TOTAL_RECORDS_11 = "/instances_10_totalRecords_11.json";
  private static final String INSTANCES_11 = "/instances_11_totalRecords_100.json";
  private static final String SRS_RECORD_WITH_INVALID_JSON = "/srs_record_with_invalid_json.json";
  private static final String TWO_RECORDS_ONE_CANNOT_BE_CONVERTED_TO_XML_JSON = "/two_records_one_cannot_be_converted_to_xml.json";
  private static final String INVALID_SRS_RECORD_INSTANCE_ID_JSON = "invalid_srs_record_instance_id.json";
  private static final String CONFIG_TEST = "/configurations.entries/config_test.json";
  private static final String CONFIG_EMPTY = "/configurations.entries/config_empty.json";
  private static final String CONFIG_OAI_TENANT = "/configurations.entries/config_oaiTenant.json";
  private static final String CONFIG_OAI_TENANT_PROCESS_SUPPRESSED_RECORDS = "/configurations.entries/config_process_suppressed_records.json";
  private static final String CONFIG_WITH_INVALID_VALUE_FOR_DELETED_RECORDS = "/configurations.entries/config_invalid_setting_value.json";
  private static final String CONFIGURATIONS_ENTRIES = "/configurations/entries";

  private static final String INSTANCE_STORAGE_URI = "/instance-storage/instances";
  private static final String SOURCE_STORAGE_RESULT_URI = "/source-storage/source-records";
  private static final String STREAMING_INVENTORY_INSTANCE_IDS_ENDPOINT = "/inventory-hierarchy/items-and-holdings";
  private static final String STREAMING_INVENTORY_ITEMS_AND_HOLDINGS_ENDPOINT = "/inventory-hierarchy/updated-instance-ids";

  public static final String ERROR_TENANT = "error";
  public static final String INVALID_CONFIG_TENANT = "invalid_config_value_tenant";

  private static final String LOCATION_JSON_PATH = "/filtering-conditions/locations.json";
  private static final String ILL_POLICIES_JSON_PATH = "/filtering-conditions/illPolicies.json";
  private static final String MATERIAL_TYPES_JSON_PATH = "/filtering-conditions/materialTypes.json";
  private static final String INSTANCE_TYPES_JSON_PATH = "/filtering-conditions/instanceTypes.json";
  private static final String INSTANCE_FORMATS_JSON_PATH = "/filtering-conditions/instanceFormats.json";
  private static final String ELECTRONIC_ACCESS_RELATIONSHIPS_JSON_PATH = "/filtering-conditions/electronicAccessRelationships.json";
  private static final String ALTERNATIVE_TITLE_TYPES_JSON_PATH = "/filtering-conditions/alternativeTitleTypes.json";
  private static final String CALL_NUMBER_TYPES_JSON_PATH = "/filtering-conditions/callNumberTypes.json";
  private static final String CAMPUSES_JSON_PATH = "/filtering-conditions/campuses.json";
  private static final String CONTRIBUTOR_NAME_TYPES_JSON_PATH = "/filtering-conditions/contributorNameTypes.json";
  private static final String HOLDINGS_NOTE_TYPES_JSON_PATH = "/filtering-conditions/holdingsNoteTypes.json";
  private static final String IDENTIFIER_TYPES_JSON_PATH = "/filtering-conditions/identifierTypes.json";
  private static final String INSTITUTIONS_JSON_PATH = "/filtering-conditions/institutions.json";
  private static final String ITEM_NOTE_TYPES_JSON_PATH = "/filtering-conditions/itemNoteTypes.json";
  private static final String LIBRARIES_JSON_PATH= "/filtering-conditions/libraries.json";
  private static final String LOAN_TYPES_JSON_PATH = "/filtering-conditions/loanTypes.json";
  private static final String MODES_OF_ISSUANCE_JSON_PATH = "/filtering-conditions/modesOfIssuance.json";
  private static final String NATURE_OF_CONTENT_TERMS_JSON_PATH = "/filtering-conditions/natureOfContentTerms.json";

  private static final String INVENTORY_VIEW_PATH = "/inventory_view/";
  private static final String LIST_IDENTIFIERS_VIEW = "list_identifiers_view.json";
  private static final String LIST_IDENTIFIERS_FOLIO_AND_MARC_VIEW = "list_identifiers_folio_and_marc_view.json";
  private static final String LIST_IDENTIFIERS_100_VIEW = "list_identifiers_100_view.json";
  private static final String LIST_IDENTIFIERS_11_VIEW = "list_identifiers_11_view.json";
  private static final String ALL_INSTANCES_IDS_JSON = "instance_ids.json";
  private static final String INSTANCE_IDS_10_JSON = "10_instance_ids.json";
  private static final String INSTANCE_IDS_3_AND_1_DELETED_JSON = "instance_3_and_1_deleted.json";
  private static final String INSTANCE_IDS_10_JSON_WITHHOLDINGS = "10_instance_ids_with_holdings.json";
  private static final String INSTANCE_IDS_60_JSON = "60_instances_ids.json";
  private static final String SRS_RECORD_TEMPLATE_JSON = "/srs_record_template.json";
  private static final String SRS_RESPONSE_TEMPLATE_JSON = "/srs_response_template.json";
  private static final String INSTANCE_ID_TO_MAKE_SRS_FAIL_JSON = "instance_id_to_make_srs_fail.json";
  private static final String INSTANCE_ID_TO_MAKE_SRS_FAIL_WITH_500_JSON = "instance_id_to_make_srs_fail_with_502.json";
  private static final String INSTANCE_ID_SRS_TIMEOUT_JSON = "instance_id_to_make_srs_fail_with_timeout.json";
  private static final String EMPTY_INSTANCES_IDS_JSON = "empty_instances_ids.json";
  private static final String ERROR_FROM_ENRICHED_INSTANCES_IDS_JSON = "error_from_enrichedInstances_ids.json";
  private static final String INSTANCE_IDS = "instanceIds";
  private static final String ENRICHED_INSTANCE_TEMPLATE_JSON = "template/enriched_instance-template.json";
  private static final String ENRICHED_INSTANCE_NO_ITEMS_JSON = "enriched_instance_no_items.json";
  private static final String TWO_RECORDS_ONE_CANNOT_BE_CONVERTED_TO_XML_INSTANCE_IDS_JSON = "two_records_one_cannot_be_converted_to_xml_instance_ids.json";
  private static final String TWO_RECORDS_WITH_CYRILLIC_DATA_JSON = "/two_records_with_cyrillic_data.json";
  private static final String DEFAULT_INSTANCE_ID = "1ed91465-7a75-4d96-bf34-4dfbd89790d5";
  private static final String DEFAULT_INSTANCE_JSON = "default_instance.json";
  private static final String DEFAULT_LIST_IDENTIFIER_JSON = "default_list_identifier.json";
  private static final String SRS_RECORD = "/srs_record.json";
  private static final String DEFAULT_SRS_RECORD = "/default_srs_record.json";
  private static final String INVALID_JSON = "invalid.json";
  private static final String INSTANCE_ID_INVALID_ENRICHED_INSTANCE_JSON = "instance_id_invalid_enriched_instance.json";
  private static final String INSTANCE_IDS_UNDERLYING_SRS_RECORDS_WITH_CYRILLIC_JSON = "instance_ids_underlying_srs_records_with_cyrillic.json";
  private static final String INSTANCE_ID_ENRICH_INSTANCES_FORBIDDEN_RESPONSE_JSON = "inventory_instance_mock_forbidden_response.json";
  private static final String INSTANCE_ID_ENRICH_INSTANCES_500_RESPONSE_JSON = "inventory_instance_mock_500_response.json";
  private static final String INSTANCE_JSON_GET_RECORD_MARC21_WITH_HOLDINGS = "instance.json";
  private static final String ENRICHED_INSTANCE_JSON_GET_RECORD_MARC21_WITH_HOLDINGS = "enriched_instance.json";

  private static final String INSTANCE_ID_NO_SRS_RECORD_JSON = "instance_id_no_srs_record.json";
  private static final String INSTANCE_ID_UNDERLYING_RECORD_WITH_CYRILLIC_DATA = "ebbb759a-dd08-4bf8-b3c3-3d75b2190c41";
  private static final String INSTANCE_ID_WITHOUT_SRS_RECORD = "3a6a47ab-597d-4abe-916d-e31c723426d3";
  private static final String INSTANCE_ID_ENRICH_INSTANCES_FORBIDDEN_RESPONSE = "8f33cdf4-6a85-4877-8b99-7d5e3be910f1";
  private static final String INSTANCE_ID_ENRICH_INSTANCES_500_RESPONSE = "12d31a35-e6cf-4840-bd85-4ae51e02a741";
  private static final String INSTANCE_ID_GET_RECORD_MARC21_WITH_HOLDINGS = "00000000-0000-4a89-a2f9-78ce3145e4fc";
  private static final String INSTANCE_ID_GET_RECORD_MARC21_FROM_INVENTORY = "existing-identifier";
  private static final String INSTANCE_ID_GET_RECORD_MARC21_WITH_HOLDINGS_FROM_INVENTORY = "00000000-0000-4000-a000-000000000111";
  private static final String INSTANCES_FROM_INVENTORY_WITH_SOURCE_FOLIO = "FOLIO";


  private static final String JSON_TEMPLATE_KEY_RECORDS = "replace_with_records";
  private static final String JSON_TEMPLATE_KEY_TOTAL_COUNT = "replace_total_count";
  private static final int LAST_RECORDS_BATCH_OFFSET_VALUE = 8;
  private static final int LIMIT_VALUE_FOR_LAST_TWO_RECORDS_IN_JSON = 2;
  private static final String INTERNAL_SERVER_ERROR = "Internal Server Error";
  private static final String INSTANCE_ID_NOT_FOUND_RESPONSE = "fb3e23e5-eb7f-4b8b-b531-40e74ec9c6e9";


  private static int srsRerequestAttemptsCount = 4;
  private static int totalSrsRerequestCallsNumber = 0;


  private final int port;
  private final Vertx vertx;

  public OkapiMockServer(Vertx vertx, int port) {
    this.port = port;
    this.vertx = vertx;
  }

  public void start(VertxTestContext context) {
    HttpServer server = vertx.createHttpServer();

    server.requestHandler(defineRoutes())
      .listen(port, context.succeeding(result -> {
        logger.info("The server has started.");
        context.completeNow();
      }));
  }

  private Router defineRoutes() {
    Router router = Router.router(vertx);
    router.route()
      .handler(BodyHandler.create());

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
    router.get(ELECTRONIC_ACCESS_RELATIONSHIPS_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(ALTERNATIVE_TITLE_TYPES_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(CALL_NUMBER_TYPES_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(CAMPUSES_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(CONTRIBUTOR_NAME_TYPES_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(HOLDINGS_NOTE_TYPES_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(IDENTIFIER_TYPES_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(INSTITUTIONS_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(ITEM_NOTE_TYPES_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(LIBRARIES_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(LOANTYPES_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(MODES_OF_ISSUANCE_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);
    router.get(NATURE_OF_CONTENT_TERMS_URI)
      .handler(this::handleInventoryStorageFilteringConditionsResponse);

    router.get(SOURCE_STORAGE_RESULT_URI)
      .handler(this::handleRecordStorageResultGetResponse);

    router.get(SOURCE_STORAGE_RESULT_URI)
      .handler(this::handleSourceRecordStorageResponse);

    router.get(CONFIGURATIONS_ENTRIES)
      .handler(this::handleConfigurationModuleResponse);

    router.get(INSTANCE_STORAGE_URI)
        .handler(this::handleInstanceStorageRequest);
    //related to MarcWithHoldingsRequestHelper
    router.post(SOURCE_STORAGE_RESULT_URI)
      .handler(this::handleRecordStorageResultPostResponse);
    router.post(STREAMING_INVENTORY_INSTANCE_IDS_ENDPOINT)
      .handler(this::handleStreamingInventoryItemsAndHoldingsResponse);
    router.get(STREAMING_INVENTORY_ITEMS_AND_HOLDINGS_ENDPOINT)
      .handler(this::handleStreamingInventoryInstanceIdsResponse);
    return router;
  }

  private void handleStreamingInventoryInstanceIdsResponse(RoutingContext ctx) {
    String uri = ctx.request()
      .absoluteURI();
    if (Objects.nonNull(uri)) {
      if (uri.contains(SUPPRESSED_RECORDS_DATE)) {
        boolean shouldProcessSuppressedRecords = Boolean.parseBoolean(ctx.request().getParam(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS));
        if (shouldProcessSuppressedRecords) {
          inventoryViewSuccessResponse(ctx, INSTANCE_IDS_10_JSON);
        } else {
          inventoryViewSuccessResponse(ctx, DEFAULT_INSTANCE_JSON);
        }
      }
      if (uri.contains(DATE_INVENTORY_STORAGE_ERROR_RESPONSE)) {
        failureResponseWithForbidden(ctx);
      } else if (uri.contains(DATE_INVENTORY_10_INSTANCE_IDS)) {
        inventoryViewSuccessResponse(ctx, INSTANCE_IDS_10_JSON);
      } else if (uri.contains(INVENTORY_27_INSTANCES_IDS_DATE)) {
        inventoryViewSuccessResponse(ctx, ALL_INSTANCES_IDS_JSON);
      } else if (uri.contains(INVENTORY_60_INSTANCE_IDS_DATE)) {
        inventoryViewSuccessResponse(ctx, INSTANCE_IDS_60_JSON);
      } else if (uri.contains(DATE_SRS_ERROR_RESPONSE)) {
        inventoryViewSuccessResponse(ctx, INSTANCE_ID_TO_MAKE_SRS_FAIL_JSON);
      } else if (uri.contains(DATE_SRS_500_ERROR_RESPONSE)) {
        inventoryViewSuccessResponse(ctx, INSTANCE_ID_TO_MAKE_SRS_FAIL_WITH_500_JSON);
      } else if (uri.contains(DATE_SRS_IDLE_TIMEOUT_ERROR_RESPONSE)) {
        inventoryViewSuccessResponse(ctx, INSTANCE_ID_SRS_TIMEOUT_JSON);
      } else if (uri.contains(EMPTY_INSTANCES_IDS_DATE)) {
        inventoryViewSuccessResponse(ctx, EMPTY_INSTANCES_IDS_JSON);
      } else if (uri.contains(DATE_ERROR_FROM_ENRICHED_INSTANCES_VIEW)) {
        inventoryViewSuccessResponse(ctx, ERROR_FROM_ENRICHED_INSTANCES_IDS_JSON);
      } else if (uri.contains(SRS_RECORD_WITH_INVALID_JSON_STRUCTURE)) {
        inventoryViewSuccessResponse(ctx, INVALID_SRS_RECORD_INSTANCE_ID_JSON);
      } else if (uri.contains("2010-10-10")) {
        failureResponse(ctx, 500, "");
      } else if (uri.contains(TWO_RECORDS_WITH_ONE_INCONVERTIBLE_TO_XML)) {
        inventoryViewSuccessResponse(ctx, TWO_RECORDS_ONE_CANNOT_BE_CONVERTED_TO_XML_INSTANCE_IDS_JSON);
      } else if (uri.contains(INVALID_INSTANCE_IDS_JSON_DATE)) {
        inventoryViewSuccessResponse(ctx, INVALID_JSON);
      } else if (uri.contains(INSTANCE_ID_WITH_INVALID_ENRICHED_INSTANCE_JSON_DATE)) {
        inventoryViewSuccessResponse(ctx, INSTANCE_ID_INVALID_ENRICHED_INSTANCE_JSON);
      } else if (uri.contains(SRS_RECORDS_WITH_CYRILLIC_DATA_DATE)) {
        inventoryViewSuccessResponse(ctx, INSTANCE_IDS_UNDERLYING_SRS_RECORDS_WITH_CYRILLIC_JSON);
      } else if (uri.contains(INSTANCE_WITHOUT_SRS_RECORD_DATE)) {
        inventoryViewSuccessResponse(ctx, INSTANCE_ID_NO_SRS_RECORD_JSON);
      } else if (uri.contains(NO_ITEMS_DATE)) {
        inventoryViewSuccessResponse(ctx, ENRICHED_INSTANCE_NO_ITEMS_JSON);
      } else if (uri.contains(GET_INSTANCES_FORBIDDEN_RESPONSE_DATE)) {
        failureResponseWithForbidden(ctx);
      } else if (uri.contains(GET_INSTANCES_IDS_500_ERROR_RETURNED_FROM_STORAGE_DATE)) {
        failureResponse(ctx, 500, INTERNAL_SERVER_ERROR);
      } else if (uri.contains(GET_ENRICHED_INSTANCES_500_ERROR_RETURNED_FROM_STORAGE_DATE)) {
        inventoryViewSuccessResponse(ctx, INSTANCE_ID_ENRICH_INSTANCES_500_RESPONSE_JSON);
      } else if (uri.contains(ENRICH_INSTANCES_FORBIDDEN_RESPONSE_DATE)) {
        inventoryViewSuccessResponse(ctx, INSTANCE_ID_ENRICH_INSTANCES_FORBIDDEN_RESPONSE_JSON);
      } else if (uri.contains(TEN_INSTANCES_WITH_HOLDINGS_DATE)) {
        inventoryViewSuccessResponse(ctx, INSTANCE_IDS_10_JSON_WITHHOLDINGS);
      } else if (uri.contains(DATE_FOR_INSTANCES_10_PARTIALLY) || uri.contains(DATE_FOR_INSTANCES_10)) {
        inventoryViewSuccessResponse(ctx, LIST_IDENTIFIERS_VIEW);
      } else if (uri.contains(DATE_FOR_INSTANCES_FOLIO_AND_MARC_10)) {
        inventoryViewSuccessResponse(ctx, LIST_IDENTIFIERS_FOLIO_AND_MARC_VIEW);
      } else if (uri.contains("2003-01-01")) {
        inventoryViewSuccessResponse(ctx, LIST_IDENTIFIERS_100_VIEW);
      } else if (uri.contains(THREE_INSTANCES_DATE_WITH_ONE_MARK_DELETED_RECORD)) {
        inventoryViewSuccessResponse(ctx, INSTANCE_IDS_3_AND_1_DELETED_JSON);
      } else if (uri.contains("2011-11-11")) { // no records found
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_0));
      } else if (uri.contains(THREE_INSTANCES_DATE)) {
        inventoryViewSuccessResponse(ctx, INSTANCE_IDS_3_AND_1_DELETED_JSON);
      } else if (uri.contains(DEFAULT_RECORD_DATE)) {
        inventoryViewSuccessResponse(ctx, DEFAULT_LIST_IDENTIFIER_JSON);
      } else if (uri.contains("deletedRecordSupport=false&skipSuppressedFromDiscoveryRecords=true&onlyInstanceUpdateDate=true")) {
        inventoryViewSuccessResponse(ctx, LIST_IDENTIFIERS_11_VIEW);
      }
      else {
        logger.debug("No mocks for the response, returning the default instance id.");
        inventoryViewSuccessResponse(ctx, DEFAULT_INSTANCE_JSON);
      }
    }
  }


  private void handleStreamingInventoryItemsAndHoldingsResponse(RoutingContext ctx) {
    JsonArray instanceIds = ctx.getBody()
      .toJsonObject()
      .getJsonArray(INSTANCE_IDS);
    logger.debug("Before building response for enriched instances, instanceIds: {}.", String.join(",", instanceIds.getList()));
    if (instanceIds.contains(INSTANCE_ID_TO_FAIL_ENRICHED_INSTANCES_REQUEST)) {
      failureResponseWithForbidden(ctx);
    } else if (instanceIds.contains(INSTANCE_ID_RELATED_ENRICHED_INSTANCE_HAS_INVALID_JSON)) {
      successResponse(ctx, getJsonObjectFromFileAsString(INVENTORY_VIEW_PATH + INVALID_JSON));
    } else if (instanceIds.isEmpty()) {
      successResponse(ctx, "");
    } else if (instanceIds.contains(INSTANCE_ID_RELATED_ENRICHED_INSTANCE_HAS_NO_ITEMS)) {
      inventoryViewSuccessResponse(ctx, ENRICHED_INSTANCE_NO_ITEMS_JSON);
    } else if (instanceIds.contains(INSTANCE_ID_ENRICH_INSTANCES_FORBIDDEN_RESPONSE)) {
      failureResponseWithForbidden(ctx);
    } else if (instanceIds.contains(INSTANCE_ID_ENRICH_INSTANCES_500_RESPONSE)) {
      failureResponse(ctx, 500, INTERNAL_SERVER_ERROR);
    } else if (instanceIds.contains(INSTANCE_ID_GET_RECORD_MARC21_WITH_HOLDINGS)) {
      successResponse(ctx, getJsonObjectFromFileAsString(INVENTORY_VIEW_PATH + ENRICHED_INSTANCE_JSON_GET_RECORD_MARC21_WITH_HOLDINGS));
    } else {
      inventoryViewSuccessResponse(ctx, instanceIds);
    }
  }

  private void handleConfigurationModuleResponse(RoutingContext ctx) {
    switch (ctx.request()
      .getHeader(OKAPI_TENANT)) {
    case EXIST_CONFIG_TENANT:
      successResponse(ctx, getJsonObjectFromFileAsString(CONFIG_TEST));
      break;
    case EXIST_CONFIG_TENANT_2:
      successResponse(ctx, getJsonObjectFromFileAsString(CONFIG_OAI_TENANT));
      break;
    case INVALID_CONFIG_TENANT:
      successResponse(ctx, getJsonObjectFromFileAsString(CONFIG_WITH_INVALID_VALUE_FOR_DELETED_RECORDS));
      break;
    case OAI_TEST_TENANT:
      if (ctx.request().absoluteURI().contains(SUPPRESSED_RECORDS_DATE)) {
        successResponse(ctx, getJsonObjectFromFileAsString(CONFIG_OAI_TENANT_PROCESS_SUPPRESSED_RECORDS));
      } else {
        successResponse(ctx, getJsonObjectFromFileAsString(CONFIG_OAI_TENANT));
      }
      break;
    case ERROR_TENANT:
      failureResponse(ctx, 500, "Internal Server Error");
      break;
    case INVALID_JSON_TENANT:
      successResponse(ctx, "&&@^$%^@$^&$");
      break;
    default:
      successResponse(ctx, getJsonObjectFromFileAsString(CONFIG_EMPTY));
      break;
    }
  }

  private void handleSourceRecordStorageResponse(RoutingContext ctx) {
    String json = getJsonObjectFromFileAsString(String.format("/source-storage/records/%s", String.format("marc-%s.json", JSON_FILE_ID)));
    if (isNotEmpty(json)) {
      final String uri = ctx.request()
        .absoluteURI();

      if (uri.contains(THREE_INSTANCES_DATE_WITH_ONE_MARK_DELETED_RECORD)) {
        json = getRecordJsonWithDeletedTrue(json);
      } else if (uri.contains(DATE_FOR_INSTANCES_10)) {
        json = getRecordJsonWithSuppressedTrue(json);
      } else if (uri.contains(THREE_INSTANCES_DATE)) {
        json = getRecordJsonWithSuppressedTrue(getRecordJsonWithDeletedTrue(json));
      } else if (uri.contains(NO_RECORDS_DATE)) {
        json = getJsonObjectFromFileAsString("/instance-storage.instances" + "/instances_0.json");
      } else if (ctx.request()
        .method() == HttpMethod.POST) {
        json = getJsonObjectFromFileAsString("/source-storage/records/srs_instances.json");
      }
      successResponse(ctx, json);
    } else {
      fail("There is no mock response");
    }
  }

  private void handleInstanceStorageRequest(RoutingContext ctx) {
    String uri = ctx.request().uri();
    if (uri.contains(INSTANCE_ID_GET_RECORD_MARC21_WITH_HOLDINGS)) {
      successResponse(ctx, getJsonObjectFromFileAsString(INVENTORY_VIEW_PATH + INSTANCE_JSON_GET_RECORD_MARC21_WITH_HOLDINGS));
    } else if (uri.contains(INSTANCE_ID_NOT_FOUND_RESPONSE)) {
      failureResponse(ctx, 404, "Not found");
    } else  if (uri.contains(INSTANCE_ID_GET_RECORD_MARC21_FROM_INVENTORY)) {
      successResponse(ctx, getJsonObjectFromFileAsString(INSTANCE_STORAGE_URI + INSTANCE_WITH_SOURCE_FOLIO));
    } else if (uri.contains(INSTANCE_ID_GET_RECORD_MARC21_WITH_HOLDINGS_FROM_INVENTORY) &&
      !uri.contains("10000000-0000-4000-a000-000000000222")) {
      successResponse(ctx, getJsonObjectFromFileAsString(INVENTORY_VIEW_PATH + ENRICHED_INSTANCE_JSON_GET_RECORD_MARC21_WITH_HOLDINGS));
    } else  if (uri.contains(INSTANCES_FROM_INVENTORY_WITH_SOURCE_FOLIO)) {
      successResponse(ctx, getJsonObjectFromFileAsString(INSTANCE_STORAGE_URI + INSTANCES_WITH_SOURCE_FOLIO));
    } else {
      failureResponse(ctx, 500, "Internal Server Error");
    }
  }

  private void handleRecordStorageResultPostResponse(RoutingContext ctx) {
    JsonArray instanceIds = ctx.getBody()
      .toJsonArray();
    if (instanceIds.contains(INSTANCE_ID_TO_MAKE_SRS_FAIL)) {
      failureResponseWithForbidden(ctx);
    } else if (instanceIds.contains(INSTANCE_ID_TO_MAKE_SRS_FAIL_WITH_500)) {
      totalSrsRerequestCallsNumber++;
      if (srsRerequestAttemptsCount > 0) {
        srsRerequestAttemptsCount--;
        failureResponse(ctx, 502, "Bad Gateway");
      } else {
        srsRerequestAttemptsCount = 4;
        successResponse(ctx, generateSrsPostResponseForInstanceIds(instanceIds));
      }
    } else if (instanceIds.contains(INSTANCE_ID_TO_MAKE_SRS_FAIL_BY_TIMEOUT)) {
      totalSrsRerequestCallsNumber++;
      if (srsRerequestAttemptsCount > 0) {
        srsRerequestAttemptsCount--;
        vertx.setTimer(3000, timerId -> successResponse(ctx, ""));
      } else {
        srsRerequestAttemptsCount = 4;
        successResponse(ctx, generateSrsPostResponseForInstanceIds(instanceIds));
      }
    } else if (instanceIds.contains(INVALID_SRS_RECORD_INSTANCE_ID)) {
      successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + SRS_RECORD_WITH_INVALID_JSON));
    } else if (instanceIds.contains(TWO_RECORDS_WITH_ONE_INCONVERTIBLE_TO_XML_INSTANCE_ID)) {
      successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + TWO_RECORDS_ONE_CANNOT_BE_CONVERTED_TO_XML_JSON));
    } else if (instanceIds.contains(DEFAULT_INSTANCE_ID)) {
      successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + DEFAULT_SRS_RECORD));
    } else if (instanceIds.contains(INSTANCE_ID_UNDERLYING_RECORD_WITH_CYRILLIC_DATA)) {
      successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + TWO_RECORDS_WITH_CYRILLIC_DATA_JSON));
    } else if (instanceIds.contains(INSTANCE_ID_WITHOUT_SRS_RECORD)) {
      successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_0));
    } else {
      String mockSrsResponse = generateSrsPostResponseForInstanceIds(instanceIds);
      successResponse(ctx, mockSrsResponse);
    }
  }

  private void handleRecordStorageResultGetResponse(RoutingContext ctx) {
    String uri = ctx.request()
      .absoluteURI();
    if (uri != null) {
      if (uri.contains(DEFAULT_RECORD_DATE)) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + DEFAULT_SRS_RECORD));
      } else if (uri.contains(String.format("%s=%s", ID_PARAM, EXISTING_IDENTIFIER))
          || uri.contains(String.format("%s=%s", ID_PARAM, RECORD_IDENTIFIER_MARC21_WITH_HOLDINGS))) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_1));
      } else if (uri.contains(String.format("%s=%s", ID_PARAM, RECORD_IDENTIFIER_INSTANCE_NOT_FOUND))) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + SRS_RECORD_WITH_NON_EXISTING_INSTANCE_JSON));
      } else if (uri.contains(String.format("%s=%s", ID_PARAM, NON_EXISTING_IDENTIFIER))) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_0));
      } else if (uri.contains(NO_RECORDS_DATE_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_0));
      } else if (uri.contains(String.format("%s=%s", ID_PARAM, ERROR_IDENTIFIER))) {
        failureResponse(ctx, 500, "Internal Server Error");
      } else if (uri.contains(ERROR_UNTIL_DATE_STORAGE)) {
        failureResponse(ctx, 500, "Internal Server Error");
      } else if (uri.contains(PARTITIONABLE_RECORDS_DATE_TIME_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_11));
      } else if (uri.contains(DATE_FOR_ONE_INSTANCE_BUT_WITHOT_RECORD_STORAGE) || uri.contains(NOT_FOUND_RECORD_INSTANCE_ID)) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_1_NO_RECORD_SOURCE));
      } else if (uri.contains(RECORD_STORAGE_INTERNAL_SERVER_ERROR_UNTIL_DATE_STORAGE)) {
        failureResponse(ctx, 500, "Internal Server Error");
      } else if (uri.contains(THREE_INSTANCES_DATE)) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_3));
      } else if (uri.contains(DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOT_RECORD_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_4));
      } else if (uri.contains(DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOUT__EXTERNAL_IDS_HOLDER_FIELD_STORAGE)) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_3_LAST_WITHOUT_EXTERNAL_IDS_HOLDER_FIELD));
      } else if (uri.contains(DATE_FOR_INSTANCES_10) || uri.contains(DATE_FOR_INSTANCES_FOLIO_AND_MARC_10)) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_10_TOTAL_RECORDS_10));
      } else if (uri.contains(DATE_FOR_INSTANCES_10_PARTIALLY)) {
        int offset = parseInt(ctx.request().getParam("offset"));
        int limit = parseInt(ctx.request().getParam("limit"));
        successResponse(ctx, getSrsRecordsPartially(ctx.request().params()));
      } else if (uri.contains(THREE_INSTANCES_DATE_WITH_ONE_MARK_DELETED_RECORD)) {
        String json = getJsonWithRecordMarkAsDeleted(getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_3));
        successResponse(ctx, json);
      } else if (uri.contains(THREE_INSTANCES_DATE_WITH_ONE_MARK_DELETED_RECORD_LIST_RECORDS)) {
        String json = getJsonWithRecordMarkAsDeleted(getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_3_WITH_DELETED));
        successResponse(ctx, json);
      } else if (uri.contains(SRS_RECORD_WITH_INVALID_JSON_STRUCTURE)) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + SRS_RECORD_WITH_INVALID_JSON));
      } else if (uri.contains(TWO_RECORDS_WITH_ONE_INCONVERTIBLE_TO_XML)) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + TWO_RECORDS_ONE_CANNOT_BE_CONVERTED_TO_XML_JSON));
      } else if (uri.contains(SRS_RECORDS_WITH_CYRILLIC_DATA_DATE)) {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + TWO_RECORDS_WITH_CYRILLIC_DATA_JSON));
      } else if (uri.contains(SRS_RECORD_WITH_OLD_METADATA_DATE)) {
        String json = getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + SRS_RECORD);
        successResponse(ctx, json.replaceAll("REPLACE_ME", OLD_METADATA_DATE_FORMAT));
      } else if (uri.contains(SRS_RECORD_WITH_NEW_METADATA_DATE)) {
        String json = getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + SRS_RECORD);
        successResponse(ctx, json.replaceAll("REPLACE_ME", NEW_METADATA_DATE_FORMAT));
      } else {
        successResponse(ctx, getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_10_TOTAL_RECORDS_11));
      }
      logger.info("Mock returns http status code: {}", ctx.response()
        .getStatusCode());
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private String getSrsRecordsPartially(MultiMap params) {
    try {
      int offset = parseInt(requireNonNull(params.get("offset")));
      int limit = parseInt(requireNonNull(params.get("limit")));
      String sourceRecordsString = requireNonNull(getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + INSTANCES_10_TOTAL_RECORDS_10));
      String srsRecordsResponseTemplate = requireNonNull(getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + SRS_RESPONSE_TEMPLATE_JSON));
      JsonObject sourceRecordsJson = new JsonObject(sourceRecordsString);
      JsonArray defaultRecords = sourceRecordsJson.getJsonArray("sourceRecords");
      JsonArray requiredRecords = new JsonArray();
      int interval = offset == LAST_RECORDS_BATCH_OFFSET_VALUE ? LIMIT_VALUE_FOR_LAST_TWO_RECORDS_IN_JSON : limit;
      for (int i = 0; i < interval; i++) {
        requiredRecords.add(defaultRecords.getJsonObject(offset + i));
      }
      String requiredRecordsArray = requiredRecords.encode();
      requiredRecordsArray = requiredRecordsArray.substring(1, requiredRecordsArray.length() - 1);
      String response = srsRecordsResponseTemplate.replaceAll(JSON_TEMPLATE_KEY_RECORDS, requiredRecordsArray);
      return response.replaceAll(JSON_TEMPLATE_KEY_TOTAL_COUNT, "10");
    } catch (Exception ex) {
      logger.error("Can't obtain the offset/limit params. {}", ex.getMessage());
      fail(ex);
    }
    return EMPTY;
  }

  private void handleInventoryStorageFilteringConditionsResponse(RoutingContext ctx) {
    String uri = ctx.request()
      .absoluteURI();
    if (uri.contains(ILL_POLICIES_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(ILL_POLICIES_JSON_PATH));
    } else if (uri.contains(INSTANCE_FORMATS_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(INSTANCE_FORMATS_JSON_PATH));
    } else if (uri.contains(RESOURCE_TYPES_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(INSTANCE_TYPES_JSON_PATH));
    } else if (uri.contains(LOCATION_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(LOCATION_JSON_PATH));
    } else if (uri.contains(MATERIAL_TYPES_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(MATERIAL_TYPES_JSON_PATH));
    } else if (uri.contains(ALTERNATIVE_TITLE_TYPES_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(ALTERNATIVE_TITLE_TYPES_JSON_PATH));
    } else if (uri.contains(CALL_NUMBER_TYPES_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(CALL_NUMBER_TYPES_JSON_PATH));
    } else if (uri.contains(CAMPUSES_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(CAMPUSES_JSON_PATH));
    } else if (uri.contains(CONTRIBUTOR_NAME_TYPES_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(CONTRIBUTOR_NAME_TYPES_JSON_PATH));
    } else if (uri.contains(ELECTRONIC_ACCESS_RELATIONSHIPS_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(ELECTRONIC_ACCESS_RELATIONSHIPS_JSON_PATH));
    } else if (uri.contains(HOLDINGS_NOTE_TYPES_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(HOLDINGS_NOTE_TYPES_JSON_PATH));
    } else if (uri.contains(IDENTIFIER_TYPES_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(IDENTIFIER_TYPES_JSON_PATH));
    } else if (uri.contains(INSTITUTIONS_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(INSTITUTIONS_JSON_PATH));
    } else if (uri.contains(ITEM_NOTE_TYPES_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(ITEM_NOTE_TYPES_JSON_PATH));
    } else if (uri.contains(LIBRARIES_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(LIBRARIES_JSON_PATH));
    } else if (uri.contains(LOANTYPES_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(LOAN_TYPES_JSON_PATH));
    } else if (uri.contains(MODES_OF_ISSUANCE_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(MODES_OF_ISSUANCE_JSON_PATH));
    } else if (uri.contains(NATURE_OF_CONTENT_TERMS_URI)) {
      successResponse(ctx, getJsonObjectFromFileAsString(NATURE_OF_CONTENT_TERMS_JSON_PATH));
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

  private void inventoryViewSuccessResponse(RoutingContext routingContext, String jsonFileName) {
    String path = INVENTORY_VIEW_PATH + jsonFileName;
    logger.debug("Path value: {}", path);
    Buffer buffer = Buffer.buffer(getJsonObjectFromFileAsString(path));
    logger.debug("Ending response for instance ids with buffer: {}", buffer.toString());
    routingContext.response().setStatusCode(200).end(buffer);
  }

  private void inventoryViewSuccessResponse(RoutingContext routingContext, JsonArray instanceIds) {
    logger.debug("building enriched instances response for instanceIds: {}." + String.join("," ,instanceIds.getList()));
    String response = generateEnrichedInstancesResponse(instanceIds, ENRICHED_INSTANCE_TEMPLATE_JSON);
    logger.debug("Built response: {}", response);
    Buffer buffer = Buffer.buffer(response);
    routingContext.response()
      .setStatusCode(200)
      .end(buffer);
  }

  private void failureResponseWithForbidden(RoutingContext ctx) {
    ctx.response()
      .setStatusCode(403)
      .setStatusMessage("Forbidden")
      .putHeader(HttpHeaders.CONTENT_TYPE, "text/plain")
      .end();
  }

  private void failureResponse(RoutingContext ctx, int code, String body) {
    ctx.response()
      .setStatusCode(code)
      .putHeader(HttpHeaders.CONTENT_TYPE, "text/plain")
      .end(body);
  }

  /**
   * Creates {@link JsonObject} from the json file
   *
   * @param path path to json file to read
   * @return json as string from the json file
   */
  private String getJsonObjectFromFileAsString(String path) {
    try {
      logger.debug("Loading file {}", path);
      URL resource = OkapiMockServer.class.getResource(path);
      if (resource == null) {
        return null;
      }
      File file = new File(resource.getFile());
      byte[] encoded = Files.readAllBytes(Paths.get(file.getPath()));
      return new String(encoded, StandardCharsets.UTF_8);
    } catch (IOException e) {
      logger.error("Unexpected error.", e);
      fail(e.getMessage());
    }
    return null;
  }

  private String getJsonWithRecordMarkAsDeleted(String json) {
    return json.replace("00778nam a2200217 c 4500", "00778dam a2200217 c 4500");
  }

  private String getRecordJsonWithDeletedTrue(String json) {
    return json.replace("\"deleted\": false", "\"deleted\": true");
  }

  private String getRecordJsonWithSuppressedTrue(String json) {
    return json.replace("\"suppressDiscovery\": false", "\"suppressDiscovery\": true");
  }

  private String generateEnrichedInstancesResponse(JsonArray instancesIds, String jsonFileName) {
    String enrichedInstanceTemplate = requireNonNull(getJsonObjectFromFileAsString(INVENTORY_VIEW_PATH + jsonFileName));
    List<String> enrichedInstances = instancesIds.stream()
      .map(Object::toString)
      .map(instanceId ->  enrichedInstanceTemplate.replace("set_instance_id", instanceId)
        .replace("set_instance_item_id", randomId())
        .replace("set_instance_item_campusId_id", randomId())
        .replace("set_instance_item__libraryId_id", randomId())
        .replace("set_instance_item_id_institutionId", randomId())
      )
      .collect(Collectors.toList());
    return String.join("", enrichedInstances);
  }

  private String generateSrsPostResponseForInstanceIds(JsonArray instanceIds) {
    String srsRecordTemplate = requireNonNull(getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + SRS_RECORD_TEMPLATE_JSON));
    String srsRecordsResponseTemplate = requireNonNull(
      getJsonObjectFromFileAsString(SOURCE_STORAGE_RESULT_URI + SRS_RESPONSE_TEMPLATE_JSON));
    List<String> srsRecords = new ArrayList<>();
    instanceIds.stream()
      .map(Object::toString)
      .forEach(id -> srsRecords.add(transformTemplateToRecord(requireNonNull(srsRecordTemplate), id)));
    String allRecords = String.join(",", srsRecords);
    return srsRecordsResponseTemplate.replace(JSON_TEMPLATE_KEY_RECORDS, allRecords)
      .replace(JSON_TEMPLATE_KEY_TOTAL_COUNT, String.valueOf(srsRecords.size()));
  }

  private String transformTemplateToRecord(String recordTemplate, String instanceId) {
    return recordTemplate.replace("replace_record_UUID", UUID.randomUUID()
      .toString())
      .replace("instanceId_replace", instanceId);
  }

  private String randomId() {
    return UUID.randomUUID()
      .toString();
  }

  public static int getTotalSrsCallsNumber() {
    int callsNumber = totalSrsRerequestCallsNumber;
    totalSrsRerequestCallsNumber = 0;
    return callsNumber;
  }
}
