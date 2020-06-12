package org.folio.oaipmh.helpers.records;

import static java.util.Objects.requireNonNull;
import static org.folio.oaipmh.Constants.CONTENT;
import static org.folio.oaipmh.Constants.FIELDS;
import static org.folio.oaipmh.Constants.GENERAL_INFO_FIELD_TAG_NUMBER;
import static org.folio.oaipmh.Constants.PARSED_RECORD;
import static org.folio.oaipmh.Constants.SUBFIELDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.storage.SourceRecordStorageHelper;
import org.folio.oaipmh.helpers.storage.StorageHelper;
import org.folio.rest.impl.OkapiMockServer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.common.collect.ImmutableMap;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
class RecordMetadataManagerTest {

  private static final Logger logger = LoggerFactory.getLogger(OkapiMockServer.class);

  private static final String SRS_INSTANCE_JSON_PATH = "/metadata-manager/srs_instance.json";
  private static final String SRS_INSTANCE_WITHOUT_GENERAL_INFO_FIELD_JSON_PATH = "/metadata-manager/srs_instance_without_999_field.json";
  private static final String INVENTORY_INSTANCE_WITH_ONE_ITEM_JSON_PATH = "/metadata-manager/inventory_instance_with_1_item.json";
  private static final String INVENTORY_INSTANCE_WITH_TWO_ITEMS_JSON_PATH = "/metadata-manager/inventory_instance_with_2_items.json";
  private static final String INVENTORY_INSTANCE_WITH_TWO_ELECTRONIC_ACCESSES = "/metadata-manager/inventory_instance_2_electronic_accesses.json";

  private static final String ITEM_WITH_ELECTRONIC_ACCESS_EMPTY = "/metadata-manager/electronic_access-empty.json";
  private static final String ITEM_WITH_ELECTRONIC_ACCESS_NO_DISPLAY_CONSTANT_GENERATED = "/metadata-manager/electronic_access-no_display_constant_generated.json";
  private static final String ITEM_WITH_ELECTRONIC_ACCESS_RELATED_RESOURCE = "/metadata-manager/electronic_access-related_resource.json";
  private static final String ITEM_WITH_ELECTRONIC_ACCESS_RESOURCE = "/metadata-manager/electronic_access-resource.json";
  private static final String ITEM_WITH_ELECTRONIC_ACCESS_VERSION_OF_RESOURCE = "/metadata-manager/electronic_access-version_of_resource.json";

  private static final String ELECTRONIC_ACCESS_FILED = "856";
  private static final String EFFECTIVE_LOCATION_FILED = "952";
  private static final int FIRST_INDICATOR_INDEX = 0;
  private static final int SECOND_INDICATOR_INDEX = 1;

  private RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
  private StorageHelper storageHelper = new SourceRecordStorageHelper();
  private static Request request;

  @BeforeAll
  static void setUp() {
    Map<String, String> okapiHeaders = ImmutableMap.of("tenant", "diku");
    request = Request.builder()
      .okapiHeaders(okapiHeaders)
      .build();
  }

  @Test
  void shouldUpdateRecordMetadataWithInventoryItemsDataAndItemsArrayHasOneElement() {
    JsonObject srsInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_JSON_PATH)));
    JsonObject inventoryInstance = new JsonObject(
        requireNonNull(getJsonObjectFromFile(INVENTORY_INSTANCE_WITH_ONE_ITEM_JSON_PATH)));

    JsonObject populatedWithItemsDataSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance);
    verifySrsInstanceSuccessfullyUpdated(populatedWithItemsDataSrsInstance);
  }

  @Test
  void shouldUpdateRecordMetadataWithTwoEffectiveLocationFields_whenInventoryItemsArrayHasTwoElements() {
    JsonObject srsInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_JSON_PATH)));
    JsonObject inventoryInstance = new JsonObject(
        requireNonNull(getJsonObjectFromFile(INVENTORY_INSTANCE_WITH_TWO_ITEMS_JSON_PATH)));

    JsonObject populatedWithItemsDataSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance);

    JsonArray fields = getContentFieldsArray(populatedWithItemsDataSrsInstance);
    List<JsonObject> effectiveLocationFields = getFieldsFromFieldsListByTagNumber(fields, EFFECTIVE_LOCATION_FILED);

    assertEquals(2, effectiveLocationFields.size());
    effectiveLocationFields.forEach(this::verifyEffectiveLocationFieldHasCorrectData);
  }

  @Test
  void shouldUpdateRecordMetadataWithTwoElectronicAccessFields_whenInventoryItemHasElectronicAccessArrayWithTwoItems() {
    JsonObject srsInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_JSON_PATH)));
    JsonObject inventoryInstance = new JsonObject(
        requireNonNull(getJsonObjectFromFile(INVENTORY_INSTANCE_WITH_TWO_ELECTRONIC_ACCESSES)));

    JsonObject populatedWithItemsDataSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance);

    JsonArray fields = getContentFieldsArray(populatedWithItemsDataSrsInstance);
    List<JsonObject> electronicAccessFields = getFieldsFromFieldsListByTagNumber(fields, ELECTRONIC_ACCESS_FILED);

    assertEquals(2, electronicAccessFields.size());
    electronicAccessFields.forEach(this::verifyElectronicAccessFieldHasCorrectData);
  }

  @ParameterizedTest
  @MethodSource(value = "electronicAccessRelationshipsAndExpectedIndicatorValues")
  void shouldCorrectlyResolveElectronicAccessIndicatorsValues(String jsonFilePath, List<String> expectedIndicators) {
    JsonObject srsInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_JSON_PATH)));
    JsonObject inventoryInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(jsonFilePath)));

    JsonObject populatedWithItemsDataSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance);
    JsonArray fields = getContentFieldsArray(populatedWithItemsDataSrsInstance);
    JsonObject electronicAccessField = getFieldFromFieldsListByTagNumber(fields, ELECTRONIC_ACCESS_FILED);
    JsonObject fieldContent = electronicAccessField.getJsonObject(ELECTRONIC_ACCESS_FILED);
    String firstIndicator = fieldContent.getString("ind1");
    String secondIndicator = fieldContent.getString("ind2");
    assertEquals(expectedIndicators.get(FIRST_INDICATOR_INDEX), firstIndicator);
    assertEquals(expectedIndicators.get(SECOND_INDICATOR_INDEX), secondIndicator);
  }

  @Test
  void shouldUpdateGeneralInfoFieldWithDiscoverySuppressedData_whenSettingIsON(Vertx vertx, VertxTestContext testContext) {
    System.setProperty("repository.suppressedRecordsProcessing", "true");
    vertx.runOnContext(event -> testContext.verify(() -> {
      JsonObject record = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_JSON_PATH)));
      String source = storageHelper.getInstanceRecordSource(record);
      String updatedSource = metadataManager.updateMetadataSourceWithDiscoverySuppressedDataIfNecessary(source, record, request);
      verifySourceWasUpdatedWithNewSubfield(updatedSource);
      testContext.completeNow();
    }));
  }

  @Test
  void shouldAppendGeneralInfoFieldWithDiscoverySuppressedData_whenSettingIsON(Vertx vertx, VertxTestContext testContext) {
    System.setProperty("repository.suppressedRecordsProcessing", "true");
    vertx.runOnContext(event -> testContext.verify(() -> {
      JsonObject record = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_WITHOUT_GENERAL_INFO_FIELD_JSON_PATH)));
      String source = storageHelper.getInstanceRecordSource(record);
      String updatedSource = metadataManager.updateMetadataSourceWithDiscoverySuppressedDataIfNecessary(source, record, request);
      verifySourceWasUpdatedWithNewSubfield(updatedSource);
      testContext.completeNow();
    }));
  }

  private static Stream<Arguments> electronicAccessRelationshipsAndExpectedIndicatorValues() {
    Stream.Builder<Arguments> builder = Stream.builder();
    builder.add((Arguments.arguments(ITEM_WITH_ELECTRONIC_ACCESS_NO_DISPLAY_CONSTANT_GENERATED, Arrays.asList("4", "8"))));
    builder.add((Arguments.arguments(ITEM_WITH_ELECTRONIC_ACCESS_EMPTY, Arrays.asList("4", " "))));
    builder.add((Arguments.arguments(ITEM_WITH_ELECTRONIC_ACCESS_RELATED_RESOURCE, Arrays.asList("4", "2"))));
    builder.add((Arguments.arguments(ITEM_WITH_ELECTRONIC_ACCESS_RESOURCE, Arrays.asList("4", "0"))));
    builder.add((Arguments.arguments(ITEM_WITH_ELECTRONIC_ACCESS_VERSION_OF_RESOURCE, Arrays.asList("4", "1"))));
    return builder.build();
  }

  private void verifySrsInstanceSuccessfullyUpdated(JsonObject srsInstance) {
    JsonArray fields = getContentFieldsArray(srsInstance);
    JsonObject electronicAccessField = getFieldFromFieldsListByTagNumber(fields, ELECTRONIC_ACCESS_FILED);
    JsonObject effectiveLocationField = getFieldFromFieldsListByTagNumber(fields, EFFECTIVE_LOCATION_FILED);
    verifyEffectiveLocationFieldHasCorrectData(effectiveLocationField);
    verifyElectronicAccessFieldHasCorrectData(electronicAccessField);
  }

  @SuppressWarnings("unchecked")
  private void verifyElectronicAccessFieldHasCorrectData(JsonObject field) {
    Map<String, Object> fieldMap = field.getMap();
    Map<String, Object> fieldContentMap = (Map<String, Object>) fieldMap.get(ELECTRONIC_ACCESS_FILED);
    String firstIndicator = (String) fieldContentMap.get("ind1");
    String secondIndicator = (String) fieldContentMap.get("ind2");
    assertEquals("4", firstIndicator);
    assertEquals("0", secondIndicator);

    List<Map<String, Object>> subFieldsList = (List<Map<String, Object>>) fieldContentMap.get(SUBFIELDS);
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "u", "test.com"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "y", "GS demo Holding 1_2"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "3", "Materials specified"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "z", "URL public note"));
  }

  @SuppressWarnings("unchecked")
  private void verifyEffectiveLocationFieldHasCorrectData(JsonObject field) {
    Map<String, Object> fieldMap = field.getMap();
    Map<String, Object> fieldContentMap = (Map<String, Object>) fieldMap.get(EFFECTIVE_LOCATION_FILED);
    String firstIndicator = (String) fieldContentMap.get("ind1");
    String secondIndicator = (String) fieldContentMap.get("ind2");
    assertEquals("f", firstIndicator);
    assertEquals("f", secondIndicator);

    List<Map<String, Object>> subFieldsList = (List<Map<String, Object>>) fieldContentMap.get(SUBFIELDS);
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "a", "Københavns Universitet"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "b", "City Campus"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "c", "Datalogisk Institut"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "d", "testName"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "e", "Call number 1_2_1"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "f", "prefix"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "g", "suffix"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "h", "512173a7-bd09-490e-b773-17d83f2b63fe"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "i", "book"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "j", "Volume 1_2_1"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "k", "Enumeration 1_2_1"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "l", "testChronology"));
    assertTrue(containsSubFieldWithCodeAndValue(subFieldsList, "m", "testBarcode"));
  }

  private JsonArray getContentFieldsArray(JsonObject srsInstance) {
    JsonObject parsedRecord = srsInstance.getJsonObject(PARSED_RECORD);
    JsonObject content = parsedRecord.getJsonObject(CONTENT);
    return content.getJsonArray(FIELDS);
  }

  private boolean containsSubFieldWithCodeAndValue(List<Map<String, Object>> subFieldsList, String subFieldCode,
      String subFieldValue) {
    return subFieldsList.stream()
      .anyMatch(subField -> subField.containsKey(subFieldCode) && subField.get(subFieldCode)
        .equals(subFieldValue));
  }

  private JsonObject getFieldFromFieldsListByTagNumber(JsonArray fields, String tag) {
    return fields.stream()
      .map(jsonObject -> (JsonObject) jsonObject)
      .filter(jsonObject -> jsonObject.containsKey(tag))
      .findFirst()
      .get();
  }

  private List<JsonObject> getFieldsFromFieldsListByTagNumber(JsonArray fields, String tag) {
    return fields.stream()
      .map(jsonObject -> (JsonObject) jsonObject)
      .filter(jsonObject -> jsonObject.containsKey(tag))
      .collect(Collectors.toList());
  }

  private void verifySourceWasUpdatedWithNewSubfield(String source) {
    JsonObject jsonFromSource = new JsonObject(source);
    JsonArray fields = jsonFromSource.getJsonArray(FIELDS);
    JsonObject generalInfoFiled = fields.stream()
      .map(jsonObject -> (JsonObject) jsonObject)
      .filter(metadataManager.getGeneralInfoFieldPredicate())
      .findFirst()
      .get();

    JsonObject fieldContent = generalInfoFiled.getJsonObject(GENERAL_INFO_FIELD_TAG_NUMBER);
    JsonArray subFields = fieldContent.getJsonArray(SUBFIELDS);
    JsonObject discoverySuppressedSubField = subFields.stream()
      .map(jsonObject -> (JsonObject) jsonObject)
      .filter(jsonObject -> jsonObject.containsKey("t"))
      .findFirst()
      .get();
    int subFieldValue = discoverySuppressedSubField.getInteger("t");
    assertEquals(0, subFieldValue);
  }

  /**
   * Creates {@link JsonObject} from the json file
   *
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

}