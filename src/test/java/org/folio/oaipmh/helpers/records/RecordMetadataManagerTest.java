package org.folio.oaipmh.helpers.records;

import static java.util.Objects.requireNonNull;
import static org.folio.oaipmh.Constants.CONTENT;
import static org.folio.oaipmh.Constants.FIELDS;
import static org.folio.oaipmh.Constants.PARSED_RECORD;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.SUBFIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.folio.oaipmh.helpers.storage.SourceRecordStorageHelper;
import org.folio.oaipmh.helpers.storage.StorageHelper;
import org.folio.rest.impl.OkapiMockServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
class RecordMetadataManagerTest {

  private static final Logger logger = LogManager.getLogger(OkapiMockServer.class);

  private static final String SRS_INSTANCE_JSON_PATH = "/metadata-manager/srs_instance.json";
  private static final String SRS_INSTANCE_WITH_ELECTRONIC_ACCESS = "/metadata-manager/srs_instance_with_electronic_access.json";
  private static final String INVENTORY_INSTANCE_WITH_ONE_ITEM_JSON_PATH = "/metadata-manager/inventory_instance_with_1_item.json";
  private static final String INVENTORY_INSTANCE_WITH_TWO_ITEMS_JSON_PATH = "/metadata-manager/inventory_instance_with_2_items.json";
  private static final String INVENTORY_INSTANCE_WITH_TWO_ELECTRONIC_ACCESSES = "/metadata-manager/inventory_instance_2_electronic_accesses.json";
  private static final String INVENTORY_INSTANCE_WITH_NOT_SUPPRESSED_FROM_DISCOVERY_ITEM = "/metadata-manager/inventory_instance_with_not_suppressed_from_discovery_item.json";
  private static final String INVENTORY_INSTANCE_WITH_SUPPRESSED_FROM_DISCOVERY_ITEM = "/metadata-manager/inventory_instance_with_suppressed_from_discovery_item.json";
  private static final String INVENTORY_INSTANCE_WHIT_ITEM_WITHOUT_LOCATION = "/metadata-manager/inventory_instance_whit_item_without_location.json";
  private static final String INVENTORY_INSTANCE_WITH_ITEM_WITHOUT_CALL_NUMBER = "/metadata-manager/inventory_instance_with_item_without_call_number.json";

  private static final String ITEM_WITH_ELECTRONIC_ACCESS_EMPTY = "/metadata-manager/electronic_access-empty.json";
  private static final String ITEM_WITH_ELECTRONIC_ACCESS_NO_DISPLAY_CONSTANT_GENERATED = "/metadata-manager/electronic_access-no_display_constant_generated.json";
  private static final String ITEM_WITH_ELECTRONIC_ACCESS_RELATED_RESOURCE = "/metadata-manager/electronic_access-related_resource.json";
  private static final String ITEM_WITH_ELECTRONIC_ACCESS_RESOURCE = "/metadata-manager/electronic_access-resource.json";
  private static final String ITEM_WITH_ELECTRONIC_ACCESS_VERSION_OF_RESOURCE = "/metadata-manager/electronic_access-version_of_resource.json";

  private static final String ITEM_BOTH_LOAN_TYPES = "/metadata-manager/item_both_loan_types.json";
  private static final String ITEM_ONLY_PERMANENT_LOAN_TYPE = "/metadata-manager/item_only_permanent_loan_type.json";
  private static final String ITEM_EMPTY_LOAN_TYPE = "/metadata-manager/item_empty_loan_type.json";

  private static final String ELECTRONIC_ACCESS_FILED = "856";
  private static final String GENERAL_INFO_FIELD = "999";
  private static final String EFFECTIVE_LOCATION_FILED = "952";
  private static final int FIRST_INDICATOR_INDEX = 0;
  private static final int SECOND_INDICATOR_INDEX = 1;

  private RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
  private StorageHelper storageHelper = new SourceRecordStorageHelper();

  @Test
  void shouldUpdateRecordMetadataWithInventoryItemsDataAndItemsArrayHasOneElement() {
    JsonObject srsInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_JSON_PATH)));
    JsonObject inventoryInstance = new JsonObject(
        requireNonNull(getJsonObjectFromFile(INVENTORY_INSTANCE_WITH_ONE_ITEM_JSON_PATH)));

    JsonObject populatedWithItemsDataSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance,
        true);
    verifySrsInstanceSuccessfullyUpdated(populatedWithItemsDataSrsInstance);
  }

  @Test
  void shouldUpdateRecordMetadataWithTwoEffectiveLocationFields_whenInventoryItemsArrayHasTwoElements() {
    JsonObject srsInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_WITH_ELECTRONIC_ACCESS)));
    JsonObject inventoryInstance = new JsonObject(
        requireNonNull(getJsonObjectFromFile(INVENTORY_INSTANCE_WITH_TWO_ITEMS_JSON_PATH)));

    JsonObject populatedWithItemsDataSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance,
        true);

    JsonArray fields = getContentFieldsArray(populatedWithItemsDataSrsInstance);
    List<JsonObject> effectiveLocationFields = getFieldsFromFieldsListByTagNumber(fields, EFFECTIVE_LOCATION_FILED);

    assertEquals(2, effectiveLocationFields.size());
    effectiveLocationFields.forEach(element -> verifyEffectiveLocationFieldHasCorrectData(element, true, true));
  }

  @Test
  void shouldUpdateRecordMetadataWithTwoElectronicAccessFields_whenInventoryItemHasElectronicAccessArrayWithTwoItems() {
    JsonObject srsInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_WITH_ELECTRONIC_ACCESS)));
    JsonObject inventoryInstance = new JsonObject(
        requireNonNull(getJsonObjectFromFile(INVENTORY_INSTANCE_WITH_TWO_ELECTRONIC_ACCESSES)));

    JsonObject populatedWithItemsDataSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance,
        true);

    JsonArray fields = getContentFieldsArray(populatedWithItemsDataSrsInstance);
    List<JsonObject> electronicAccessFields = getFieldsFromFieldsListByTagNumber(fields, ELECTRONIC_ACCESS_FILED);

    // 2 from inventory, 1 from srs
    assertEquals(3, electronicAccessFields.size());
    electronicAccessFields.forEach(this::verifyElectronicAccessFieldHasCorrectData);
  }

  @ParameterizedTest
  @MethodSource(value = "electronicAccessRelationshipsAndExpectedIndicatorValues")
  void shouldCorrectlyResolveElectronicAccessIndicatorsValues(String jsonFilePath, List<String> expectedIndicators) {
    JsonObject srsInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_JSON_PATH)));
    JsonObject inventoryInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(jsonFilePath)));

    JsonObject populatedWithItemsDataSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance,
        true);
    JsonArray fields = getContentFieldsArray(populatedWithItemsDataSrsInstance);
    JsonObject electronicAccessField = getFieldFromFieldsListByTagNumber(fields, ELECTRONIC_ACCESS_FILED);
    JsonObject fieldContent = electronicAccessField.getJsonObject(ELECTRONIC_ACCESS_FILED);
    String firstIndicator = fieldContent.getString("ind1");
    String secondIndicator = fieldContent.getString("ind2");
    assertEquals(expectedIndicators.get(FIRST_INDICATOR_INDEX), firstIndicator);
    assertEquals(expectedIndicators.get(SECOND_INDICATOR_INDEX), secondIndicator);
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource(value = "itemsWithDifferentLoanTypesAndExpectedSubfieldValues")
  void shouldCorrectlySetLoanType(String jsonFilePath, String expectedValues, boolean subfieldShouldExist) {
    JsonObject srsInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_JSON_PATH)));
    JsonObject inventoryInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(jsonFilePath)));

    JsonObject populatedWithItemsDataSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance,
      true);
    JsonArray fields = getContentFieldsArray(populatedWithItemsDataSrsInstance);
    JsonObject effectiveLocation = getFieldFromFieldsListByTagNumber(fields, EFFECTIVE_LOCATION_FILED);
    Map<String, Object> fieldMap = effectiveLocation.getMap();
    Map<String, Object> effectiveLocationField = (Map<String, Object>) fieldMap.get(EFFECTIVE_LOCATION_FILED);
    List<Map<String, Object>> subfieldsList = (List<Map<String, Object>>) effectiveLocationField.get(SUBFIELDS);
    assertTrue(verifySubFieldValuePresence(subfieldsList, "p", expectedValues, subfieldShouldExist));
  }

  @Test
  void shouldUpdateFieldsWithDiscoverySuppressedData_whenSettingIsON(Vertx vertx, VertxTestContext testContext) {
    System.setProperty(REPOSITORY_SUPPRESSED_RECORDS_PROCESSING, "true");
    vertx.runOnContext(event -> testContext.verify(() -> {
      JsonObject record = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_WITH_ELECTRONIC_ACCESS)));
      String source = storageHelper.getInstanceRecordSource(record);
      String updatedSource = metadataManager.updateMetadataSourceWithDiscoverySuppressedData(source, record);
      verifySourceWasUpdatedWithNewSubfield(updatedSource, metadataManager.getGeneralInfoFieldPredicate(), GENERAL_INFO_FIELD);
      updatedSource = metadataManager.updateElectronicAccessFieldWithDiscoverySuppressedData(source, record);
      verifySourceWasUpdatedWithNewSubfield(updatedSource, metadataManager.getElectronicAccessPredicate(), ELECTRONIC_ACCESS_FILED);
      testContext.completeNow();
    }));
  }

  @Test
  void shouldUpdateFieldsWithEffectiveLocationFieldWithoutLocationsSubfieldGroup_whenInstanceHasItemWithoutLocationData() {
    JsonObject srsInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_WITH_ELECTRONIC_ACCESS)));
    JsonObject inventoryInstance = new JsonObject(
        requireNonNull(getJsonObjectFromFile(INVENTORY_INSTANCE_WHIT_ITEM_WITHOUT_LOCATION)));

    JsonObject populatedWithItemsDataSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance,
        true);

    JsonArray fields = getContentFieldsArray(populatedWithItemsDataSrsInstance);
    List<JsonObject> effectiveLocationFields = getFieldsFromFieldsListByTagNumber(fields, EFFECTIVE_LOCATION_FILED);

    effectiveLocationFields.forEach(element -> verifyEffectiveLocationFieldHasCorrectData(element, false, true));
  }

  @Test
  void shouldUpdateFieldsWithEffectiveLocationFieldWithoutCallNumberSubfieldGroup_whenInstanceHasItemWithoutCallNumber() {
    JsonObject srsInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_WITH_ELECTRONIC_ACCESS)));
    JsonObject inventoryInstance = new JsonObject(
        requireNonNull(getJsonObjectFromFile(INVENTORY_INSTANCE_WITH_ITEM_WITHOUT_CALL_NUMBER)));

    JsonObject populatedWithItemsDataSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance,
        true);

    JsonArray fields = getContentFieldsArray(populatedWithItemsDataSrsInstance);
    List<JsonObject> effectiveLocationFields = getFieldsFromFieldsListByTagNumber(fields, EFFECTIVE_LOCATION_FILED);

    effectiveLocationFields.forEach(element -> verifyEffectiveLocationFieldHasCorrectData(element, true, false));
  }

  @Test
  void shouldCorrectlySetTheSuppressDiscoveryValue_whenItemNotSuppressedFromDiscovery() {
    JsonObject srsInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_WITH_ELECTRONIC_ACCESS)));
    JsonObject inventoryInstance = new JsonObject(
        requireNonNull(getJsonObjectFromFile(INVENTORY_INSTANCE_WITH_NOT_SUPPRESSED_FROM_DISCOVERY_ITEM)));

    JsonObject populatedWithItemsDataSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance,
        true);

    JsonArray fields = getContentFieldsArray(populatedWithItemsDataSrsInstance);
    List<JsonObject> effectiveLocationFields = getFieldsFromFieldsListByTagNumber(fields, EFFECTIVE_LOCATION_FILED);
    int value = getSuppressedFromDiscoveryValue(effectiveLocationFields);
    assertEquals(0, value);
  }

  @Test
  void shouldCorrectlySetTheSuppressDiscoveryValue_whenItemSuppressedFromDiscovery() {
    JsonObject srsInstance = new JsonObject(requireNonNull(getJsonObjectFromFile(SRS_INSTANCE_WITH_ELECTRONIC_ACCESS)));
    JsonObject inventoryInstance = new JsonObject(
        requireNonNull(getJsonObjectFromFile(INVENTORY_INSTANCE_WITH_SUPPRESSED_FROM_DISCOVERY_ITEM)));

    JsonObject populatedWithItemsDataSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance,
        true);

    JsonArray fields = getContentFieldsArray(populatedWithItemsDataSrsInstance);
    List<JsonObject> effectiveLocationFields = getFieldsFromFieldsListByTagNumber(fields, EFFECTIVE_LOCATION_FILED);

    int value = getSuppressedFromDiscoveryValue(effectiveLocationFields);
    assertEquals(1, value);
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

  private static Stream<Arguments> itemsWithDifferentLoanTypesAndExpectedSubfieldValues() {
    Stream.Builder<Arguments> builder = Stream.builder();
    builder.add((Arguments.arguments(ITEM_ONLY_PERMANENT_LOAN_TYPE, "permanentLoanType value", true)));
    builder.add((Arguments.arguments(ITEM_BOTH_LOAN_TYPES, "temporaryLoanType value", true)));
    builder.add((Arguments.arguments(ITEM_EMPTY_LOAN_TYPE, "", false)));
    return builder.build();
  }

  private void verifySrsInstanceSuccessfullyUpdated(JsonObject srsInstance) {
    JsonArray fields = getContentFieldsArray(srsInstance);
    JsonObject electronicAccessField = getFieldFromFieldsListByTagNumber(fields, ELECTRONIC_ACCESS_FILED);
    JsonObject effectiveLocationField = getFieldFromFieldsListByTagNumber(fields, EFFECTIVE_LOCATION_FILED);
    verifyEffectiveLocationFieldHasCorrectData(effectiveLocationField, true, true);
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
    assertTrue(verifySubFieldValuePresence(subFieldsList, "u", "test.com", true));
    assertTrue(verifySubFieldValuePresence(subFieldsList, "y", "GS demo Holding 1_2", true));
    assertTrue(verifySubFieldValuePresence(subFieldsList, "3", "Materials specified", true));
    assertTrue(verifySubFieldValuePresence(subFieldsList, "z", "URL public note", true));
  }

  @SuppressWarnings("unchecked")
  private void verifyEffectiveLocationFieldHasCorrectData(JsonObject field, boolean locationGroupMustBePresented,
      boolean callNumberGroupMustBePresented) {
    Map<String, Object> fieldMap = field.getMap();
    Map<String, Object> fieldContentMap = (Map<String, Object>) fieldMap.get(EFFECTIVE_LOCATION_FILED);
    String firstIndicator = (String) fieldContentMap.get("ind1");
    String secondIndicator = (String) fieldContentMap.get("ind2");
    assertEquals("f", firstIndicator);
    assertEquals("f", secondIndicator);
    List<Map<String, Object>> subFieldsList = (List<Map<String, Object>>) fieldContentMap.get(SUBFIELDS);
    if (locationGroupMustBePresented) {
      assertTrue(verifySubFieldValuePresence(subFieldsList, "a", "Københavns Universitet", true));
      assertTrue(verifySubFieldValuePresence(subFieldsList, "b", "City Campus", true));
      assertTrue(verifySubFieldValuePresence(subFieldsList, "c", "Datalogisk Institut", true));
      assertTrue(verifySubFieldValuePresence(subFieldsList, "d", "testName", true));
    } else {
      assertFalse(verifySubFieldValuePresence(subFieldsList, "a", "Københavns Universitet", true));
      assertFalse(verifySubFieldValuePresence(subFieldsList, "b", "City Campus", true));
      assertFalse(verifySubFieldValuePresence(subFieldsList, "c", "Datalogisk Institut", true));
      assertFalse(verifySubFieldValuePresence(subFieldsList, "d", "testName", true));
    }
    if (callNumberGroupMustBePresented) {
      assertTrue(verifySubFieldValuePresence(subFieldsList, "e", "Call number 1_2_1", true));
      assertTrue(verifySubFieldValuePresence(subFieldsList, "f", "prefix", true));
      assertTrue(verifySubFieldValuePresence(subFieldsList, "g", "suffix", true));
      assertTrue(verifySubFieldValuePresence(subFieldsList, "h", "512173a7-bd09-490e-b773-17d83f2b63fe", true));
    } else {
      assertFalse(verifySubFieldValuePresence(subFieldsList, "e", "Call number 1_2_1", true));
      assertFalse(verifySubFieldValuePresence(subFieldsList, "f", "prefix", true));
      assertFalse(verifySubFieldValuePresence(subFieldsList, "g", "suffix", true));
      assertFalse(verifySubFieldValuePresence(subFieldsList, "h", "512173a7-bd09-490e-b773-17d83f2b63fe", true));
    }
    assertTrue(verifySubFieldValuePresence(subFieldsList, "i", "book", true));
    assertTrue(verifySubFieldValuePresence(subFieldsList, "j", "Volume 1_2_1", true));
    assertTrue(verifySubFieldValuePresence(subFieldsList, "k", "Enumeration 1_2_1", true));
    assertTrue(verifySubFieldValuePresence(subFieldsList, "l", "testChronology", true));
    assertTrue(verifySubFieldValuePresence(subFieldsList, "m", "testBarcode", true));
    assertTrue(verifySubFieldValuePresence(subFieldsList, "n", "copy number", true));
  }

  private JsonArray getContentFieldsArray(JsonObject srsInstance) {
    JsonObject parsedRecord = srsInstance.getJsonObject(PARSED_RECORD);
    JsonObject content = parsedRecord.getJsonObject(CONTENT);
    return content.getJsonArray(FIELDS);
  }

  private boolean verifySubFieldValuePresence(List<Map<String, Object>> subFieldsList, String subFieldCode,
                                              String subFieldValue, boolean subfieldShouldPresent) {
    if (subfieldShouldPresent) {
      return subFieldsList.stream()
        .anyMatch(subField -> subField.containsKey(subFieldCode) && subField.get(subFieldCode)
          .equals(subFieldValue));
    } else {
      return subFieldsList.stream()
        .noneMatch(subField -> subField.containsKey(subFieldCode) && subField.get(subFieldCode)
          .equals(subFieldValue));
    }


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

  private void verifySourceWasUpdatedWithNewSubfield(String source, Predicate<JsonObject> predicate, String tagNumber) {
    JsonObject jsonFromSource = new JsonObject(source);
    JsonArray fields = jsonFromSource.getJsonArray(FIELDS);
    JsonObject generalInfoFiled = fields.stream()
      .map(jsonObject -> (JsonObject) jsonObject)
      .filter(predicate)
      .findFirst()
      .get();

    JsonObject fieldContent = generalInfoFiled.getJsonObject(tagNumber);
    JsonArray subFields = fieldContent.getJsonArray(SUBFIELDS);
    JsonObject discoverySuppressedSubField = subFields.stream()
      .map(jsonObject -> (JsonObject) jsonObject)
      .filter(jsonObject -> jsonObject.containsKey("t"))
      .findFirst()
      .get();
    int subFieldValue = discoverySuppressedSubField.getInteger("t");
    assertEquals(0, subFieldValue);
  }

  private int getSuppressedFromDiscoveryValue(List<JsonObject> effectiveLocationFields) {
    Optional<JsonObject> optionalSuppressFromDiscoverySubfield = effectiveLocationFields.iterator()
      .next()
      .getJsonObject("952")
      .getJsonArray("subfields")
      .stream()
      .map(JsonObject.class::cast)
      .filter(jsonObject -> jsonObject.containsKey("t"))
      .findFirst();
    if (optionalSuppressFromDiscoverySubfield.isPresent()) {
      JsonObject suppressFromDiscoverySubfield = optionalSuppressFromDiscoverySubfield.get();
      return suppressFromDiscoverySubfield.getInteger("t");
    } else {
      return -1;
    }
  }

  /**
   * Creates {@link JsonObject} from the json file
   *
   * @param path path to json file to read
   * @return json as string from the json file
   */
  private String getJsonObjectFromFile(String path) {
    try {
      logger.debug("Loading file {}.", path);
      URL resource = OkapiMockServer.class.getResource(path);
      if (resource == null) {
        return null;
      }
      File file = new File(resource.getFile());
      byte[] encoded = Files.readAllBytes(Paths.get(file.getPath()));
      return new String(encoded, StandardCharsets.UTF_8);
    } catch (IOException e) {
      logger.error("Error occurred while reading the file \"{}\".", path, e);
      fail(e.getMessage());
    }
    return null;
  }

}
