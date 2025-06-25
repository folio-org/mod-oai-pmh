package org.folio.oaipmh.helpers.records;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.CONTENT;
import static org.folio.oaipmh.Constants.FIELDS;
import static org.folio.oaipmh.Constants.FIRST_INDICATOR;
import static org.folio.oaipmh.Constants.PARSED_RECORD;
import static org.folio.oaipmh.Constants.SECOND_INDICATOR;
import static org.folio.oaipmh.Constants.SUBFIELDS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.helpers.storage.StorageHelper;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Is used for manipulating with record metadata. Updates, constructs the new
 * fields or already presented fields.
 */
@Log4j2
public class RecordMetadataManager {

  private static final String GENERAL_INFO_FIELD_TAG_NUMBER = "999";
  private static final String ELECTRONIC_ACCESS_FILED_TAG_NUMBER = "856";
  private static final String EFFECTIVE_LOCATION_FILED_TAG_NUMBER = "952";

  private static final String INDICATOR_VALUE = "f";
  private static final String DISCOVERY_SUPPRESSED_SUBFIELD_CODE = "t";
  private static final String LOCATION_DISCOVERY_DISPLAY_NAME_OR_LOCATION_NAME_SUBFIELD_CODE = "d";
  private static final String LOAN_TYPE_SUBFIELD_CODE = "p";
  private static final String ILL_POLICY_SUBFIELD_CODE = "r";
  private static final String LOCATION_NAME_SUBFIELD_CODE = "s";

  private static final int FIRST_INDICATOR_INDEX = 0;
  private static final int SECOND_INDICATOR_INDEX = 1;
  private static final String LOCATION = "location";

  private static final String DEFAULT_INDICATORS = "4, ";

  private StorageHelper storageHelper = StorageHelper.getInstance();
  private final Map<String, String> indicatorsMap;
  private final Predicate<JsonObject> generalInfoFieldPredicate;
  private final Predicate<JsonObject> electronicAccessPredicate;
  private static RecordMetadataManager instance;

  private static final String PERMANENT_LOAN_TYPE = "permanentLoanType";
  private static final String TEMPORARY_LOAN_TYPE = "temporaryLoanType";
  private static final String HOLDINGS_RECORD_ID = "holdingsRecordId";
  private static final String ILL_POLICY = "illPolicy";
  public static final String ITEMS_AND_HOLDINGS_FIELDS = "itemsandholdingsfields";
  public static final String INVENTORY_SUPPRESS_DISCOVERY_FIELD = "suppressFromDiscovery";
  public static final String ITEMS = "items";
  public static final String HOLDINGS = "holdings";
  public static final String CALL_NUMBER = "callNumber";
  public static final String ELECTRONIC_ACCESS = "electronicAccess";
  public static final String NAME = "name";
  public static final String LOCATION_NAME = "locationName";
  public static final String SUPPRESS_DISCOVERY_CODE = "t";

  private RecordMetadataManager() {
    indicatorsMap = new HashMap<>();
    indicatorsMap.put("No display constant generated", "4,8");
    indicatorsMap.put("", DEFAULT_INDICATORS);
    indicatorsMap.put("Related resource", "4,2");
    indicatorsMap.put("Resource", "4,0");
    indicatorsMap.put("Version of resource", "4,1");
    indicatorsMap.put("No information provided", DEFAULT_INDICATORS);
    indicatorsMap.put("Component part(s) of resource", "4,3");
    indicatorsMap.put("Version of component part(s) of resource", "4,4");

    generalInfoFieldPredicate = jsonObject -> {
      if (jsonObject.containsKey(GENERAL_INFO_FIELD_TAG_NUMBER)) {
        JsonObject dataFieldContent = jsonObject.getJsonObject(GENERAL_INFO_FIELD_TAG_NUMBER);
        String firstIndicator = dataFieldContent.getString(FIRST_INDICATOR);
        String secondIndicator = dataFieldContent.getString(SECOND_INDICATOR);
        return StringUtils.isNotBlank(firstIndicator) && StringUtils.isNotBlank(secondIndicator)
            && firstIndicator.equals(secondIndicator) && firstIndicator.equals(INDICATOR_VALUE);
      }
      return false;
    };

    electronicAccessPredicate = jsonObject -> {
      if (jsonObject.containsKey(ELECTRONIC_ACCESS_FILED_TAG_NUMBER)) {
        JsonObject dataFieldContent = jsonObject.getJsonObject(ELECTRONIC_ACCESS_FILED_TAG_NUMBER);
        String firstIndicator = dataFieldContent.getString(FIRST_INDICATOR);
        String secondIndicator = dataFieldContent.getString(SECOND_INDICATOR);
        return StringUtils.isNotBlank(firstIndicator) && StringUtils.isNotEmpty(secondIndicator);
      }
      return false;
    };
  }

  public static RecordMetadataManager getInstance() {
    if (nonNull(instance)) {
      return instance;
    }
    instance = new RecordMetadataManager();
    return instance;
  }

  /**
   * Updates metadata of retrieved from SRS record with related to it inventory
   * items data.
   *
   * @param srsInstance       - record from SRS
   * @param inventoryInstance - instance form inventory storage
   */
  public JsonObject populateMetadataWithItemsData(JsonObject srsInstance,
      JsonObject inventoryInstance,
      boolean suppressedRecordsProcessing) {
    Object value = inventoryInstance.getValue(ITEMS_AND_HOLDINGS_FIELDS);
    if (!(value instanceof JsonObject)) {
      return srsInstance;
    }
    JsonObject itemsAndHoldings = (JsonObject) value;
    JsonArray items = itemsAndHoldings.getJsonArray(ITEMS);
    JsonArray holdings = itemsAndHoldings.getJsonArray(HOLDINGS);

    if (nonNull(items) && CollectionUtils.isNotEmpty(items.getList())) {
      List<Object> fieldsList = getFieldsForUpdate(srsInstance);
      populateItemsAndAddIllPolicy(items, holdings, fieldsList, suppressedRecordsProcessing);
      if (nonNull(holdings)) {
        populateHoldingsWithIllPolicy(items, holdings, fieldsList, suppressedRecordsProcessing);
      }
    }
    return srsInstance;
  }

  private void populateItemsAndAddIllPolicy(JsonArray items, JsonArray holdings, List<Object> fieldsList,
      boolean suppressedRecordsProcessing) {
    getItemsFromItems(items).forEach(item -> {
      var illPolicyOpt = nonNull(holdings)
          ? holdings.stream().map(JsonObject.class::cast).filter(hold -> hold.getString("id")
              .equals(item.getString(HOLDINGS_RECORD_ID)) && StringUtils.isNotBlank(hold.getString(ILL_POLICY)))
              .map(hold -> hold.getString(ILL_POLICY)).findFirst()
          : Optional.<String>empty();
      updateFieldsWithItemEffectiveLocationField(item, fieldsList, suppressedRecordsProcessing, illPolicyOpt);
      updateFieldsWithElectronicAccessField(item, fieldsList, suppressedRecordsProcessing);
    });
  }

  private void populateHoldingsWithIllPolicy(JsonArray items, JsonArray holdings, List<Object> fieldsList,
      boolean suppressedRecordsProcessing) {
    var onlyHoldings = getHoldingsWithoutItems(holdings, items);
    var holdingsFromItems = getHoldingsFromItems(items);
    if (onlyHoldings.size() == holdingsFromItems.size()) {
      IntStream.range(0, onlyHoldings.size()).forEach(pos -> {
        var illPolicyOpt = ofNullable(onlyHoldings.get(pos).getString(ILL_POLICY));
        var itemJson = holdingsFromItems.get(pos);
        updateFieldsWithItemEffectiveLocationField(itemJson, fieldsList, suppressedRecordsProcessing, illPolicyOpt);
      });
    }
  }

  private List<JsonObject> getItemsFromItems(JsonArray items) {
    return items.stream().map(JsonObject.class::cast).filter(item -> item.containsKey("id"))
        .collect(Collectors.toList());
  }

  private List<JsonObject> getHoldingsFromItems(JsonArray items) {
    return items.stream().map(JsonObject.class::cast).filter(item -> !item.containsKey(HOLDINGS_RECORD_ID))
        .collect(Collectors.toList());
  }

  private List<JsonObject> getHoldingsWithoutItems(JsonArray holdings, JsonArray items) {
    return holdings.stream().map(JsonObject.class::cast).filter(hold -> !holdingsContainsItem(hold, items))
        .collect(Collectors.toList());
  }

  private boolean holdingsContainsItem(JsonObject hold, JsonArray items) {
    return items.stream().map(JsonObject.class::cast)
        .anyMatch(item -> ofNullable(item.getString(HOLDINGS_RECORD_ID)).orElse(EMPTY)
            .equals(hold.getString("id")));
  }

  /**
   * Updates metadata of retrieved from SRS record with related to it inventory
   * holdings data.
   *
   * @param srsInstance       - record from SRS
   * @param inventoryInstance - instance form inventory storage
   */
  public JsonObject populateMetadataWithHoldingsData(JsonObject srsInstance,
      JsonObject inventoryInstance,
      boolean suppressedRecordsProcessing) {
    Object value = inventoryInstance.getValue(ITEMS_AND_HOLDINGS_FIELDS);
    if (!(value instanceof JsonObject)) {
      return srsInstance;
    }
    JsonObject itemsAndHoldings = (JsonObject) value;
    JsonArray holdings = itemsAndHoldings.getJsonArray(HOLDINGS);

    if (nonNull(holdings) && CollectionUtils.isNotEmpty(holdings.getList())) {
      List<Object> fieldsList = getFieldsForUpdate(srsInstance);
      holdings.forEach(holding -> updateFieldsWithElectronicAccessField((JsonObject) holding, fieldsList,
          suppressedRecordsProcessing));
    }
    return srsInstance;
  }

  @SuppressWarnings("unchecked")
  private List<Object> getFieldsForUpdate(JsonObject srsInstance) {
    if (!srsInstance.containsKey(PARSED_RECORD)) {
      return new ArrayList<>();
    }
    JsonObject parsedRecord = srsInstance.getJsonObject(PARSED_RECORD);
    JsonObject content = parsedRecord.getJsonObject(CONTENT);
    JsonArray fields = content.getJsonArray(FIELDS);
    return fields.getList();
  }

  /**
   * Constructs field with subfields which is build from item location data.
   * Constructed field has tag number = 952 and both
   * indicators has 'f' value.
   *
   * @param itemData                    - json of single item
   * @param marcRecordFields            - fields list to be updated with new one
   * @param suppressedRecordsProcessing - include suppressed flag in 952 field?
   * @param illPolicy                   - include illPolicy if present
   */
  private void updateFieldsWithItemEffectiveLocationField(JsonObject itemData,
      List<Object> marcRecordFields,
      boolean suppressedRecordsProcessing,
      Optional<String> illPolicy) {
    Map<String, Object> effectiveLocationSubFields = constructEffectiveLocationSubFieldsMap(itemData);
    if (suppressedRecordsProcessing) {
      effectiveLocationSubFields.put(DISCOVERY_SUPPRESSED_SUBFIELD_CODE,
          calculateDiscoverySuppressedSubfieldValue(itemData));
    }
    if (illPolicy.isPresent()) {
      effectiveLocationSubFields.put(ILL_POLICY_SUBFIELD_CODE, illPolicy.get());
    }
    FieldBuilder fieldBuilder = new FieldBuilder();
    Map<String, Object> effectiveLocationField = fieldBuilder.withFieldTagNumber(EFFECTIVE_LOCATION_FILED_TAG_NUMBER)
        .withFirstIndicator(INDICATOR_VALUE)
        .withSecondIndicator(INDICATOR_VALUE)
        .withSubFields(effectiveLocationSubFields)
        .build();
    marcRecordFields.add(effectiveLocationField);
  }

  /**
   * Calculates discovery suppressed subfield value.
   *
   * @param itemData - json of single item
   * @return subfield value
   */
  private int calculateDiscoverySuppressedSubfieldValue(JsonObject itemData) {
    if (isNull(itemData.getBoolean(INVENTORY_SUPPRESS_DISCOVERY_FIELD))) {
      return 0;
    }
    return BooleanUtils.isFalse(itemData.getBoolean(INVENTORY_SUPPRESS_DISCOVERY_FIELD)) ? 0 : 1;
  }

  /**
   * Constructs field with subfields which is build from item electronic access
   * data. Constructed field has tag number = 856 and
   * both indicators depends on 'name' field of electronic access json (see
   * {@link RecordMetadataManager#resolveIndicatorsValue}).
   *
   * @param jsonData                    - json of single item or holding which
   *                                    contains array of electronic accesses
   * @param marcRecordFields            - fields list to be updated with new one
   * @param suppressedRecordsProcessing - include suppressed flag in 856 field?
   */
  private void updateFieldsWithElectronicAccessField(JsonObject jsonData,
      List<Object> marcRecordFields,
      boolean suppressedRecordsProcessing) {
    JsonArray electronicAccessArray = jsonData.getJsonArray(ELECTRONIC_ACCESS);
    if (nonNull(electronicAccessArray)) {
      electronicAccessArray.forEach(electronicAccess -> {
        if (electronicAccess instanceof JsonObject) {
          Map<String, Object> electronicAccessSubFields = constructElectronicAccessSubFieldsMap(
              (JsonObject) electronicAccess);
          FieldBuilder fieldBuilder = new FieldBuilder();
          List<String> indicators = resolveIndicatorsValue((JsonObject) electronicAccess);
          if (suppressedRecordsProcessing) {
            int subFieldValue = BooleanUtils.isFalse(jsonData.getBoolean(INVENTORY_SUPPRESS_DISCOVERY_FIELD)) ? 0 : 1;
            electronicAccessSubFields.put(DISCOVERY_SUPPRESSED_SUBFIELD_CODE, subFieldValue);
          }
          if (CollectionUtils.isNotEmpty(indicators)) {
            Map<String, Object> electronicAccessField = fieldBuilder
                .withFieldTagNumber(ELECTRONIC_ACCESS_FILED_TAG_NUMBER)
                .withFirstIndicator(indicators.get(FIRST_INDICATOR_INDEX))
                .withSecondIndicator(indicators.get(SECOND_INDICATOR_INDEX))
                .withSubFields(electronicAccessSubFields)
                .build();
            marcRecordFields.add(electronicAccessField);
          }
        }
      });
    }
  }

  private List<String> resolveIndicatorsValue(JsonObject electronicAccess) {
    String name = electronicAccess.getString(NAME);
    String key = StringUtils.isNotEmpty(name) ? name : EMPTY;
    String indicatorsInString = indicatorsMap.getOrDefault(key, DEFAULT_INDICATORS);
    if (indicatorsInString != null) {
      return Arrays.asList(indicatorsInString.split(","));
    } else {
      return Collections.emptyList();
    }
  }

  private Map<String, Object> constructEffectiveLocationSubFieldsMap(JsonObject itemData) {
    log.debug("Processing itemData JSON: {}", itemData.encodePrettily());
    Map<String, Object> effectiveLocationSubFields = new HashMap<>();
    JsonObject locationGroup = null;
    JsonObject outerLocation = itemData.getJsonObject(LOCATION);

    if (outerLocation == null) {
      log.warn("No location information found in item: {}", itemData.encodePrettily());
      return effectiveLocationSubFields;
    }

    log.debug("Outer Location: {}", outerLocation.encodePrettily());

    // More robust handling of isActive field
    Boolean isActiveValue = null;
    if (outerLocation.containsKey("isActive")) {
      isActiveValue = outerLocation.getBoolean("isActive");
    }

    // Determine if location is active based on explicit value or heuristics
    boolean isLocationActive;
    if (isActiveValue != null) {
      // Explicit value exists - use it
      isLocationActive = isActiveValue;
      log.debug("Location isActive status (explicit): {}", isLocationActive);
    } else {
      // Missing isActive field - apply heuristics
      // If nested location is empty or missing key data, treat as inactive
      JsonObject nestedLocation = outerLocation.getJsonObject(LOCATION);
      boolean hasCompleteData = outerLocation.containsKey("name") &&
          StringUtils.isNotBlank(outerLocation.getString("name"));

      if (nestedLocation != null && nestedLocation.isEmpty()) {
        // Empty nested location often indicates inactive location
        isLocationActive = false;
        log.debug("Location isActive status (inferred from empty nested location): {}", isLocationActive);
      } else if (!hasCompleteData) {
        // Missing basic location data suggests inactive location
        isLocationActive = false;
        log.debug("Location isActive status (inferred from incomplete data): {}", isLocationActive);
      } else {
        // Default to active if we have complete data and no explicit isActive field
        isLocationActive = true;
        log.debug("Location isActive status (default with complete data): {}", isLocationActive);
      }
    }

    // Handle nested location structure (when outer location contains inner
    // location)
    if (outerLocation.containsKey(LOCATION)) {
      Object nestedLocationObj = outerLocation.getValue(LOCATION);
      if (nestedLocationObj instanceof JsonObject) {
        JsonObject nestedLocation = (JsonObject) nestedLocationObj;
        locationGroup = nestedLocation;
        log.debug("Inner Location Group: {}",
            locationGroup.isEmpty() ? "EMPTY OBJECT" : locationGroup.encodePrettily());

        // If nested location is empty (which happens for some inactive locations),
        // we need to copy all fields from outer location
        if (locationGroup.isEmpty() && !isLocationActive) {
          log.debug("Found empty nested location for inactive location. " +
              "Copying fields from outer location to ensure display.");
          // For completely empty nested locations, use the outer location data
          locationGroup = new JsonObject(outerLocation.getMap());
        }

        // Copy key fields from outer location to location group for consistency
        if (outerLocation.containsKey("isActive")) {
          locationGroup.put("isActive", outerLocation.getBoolean("isActive"));
        }

        // Copy name field values if they don't exist in the inner location
        copyFieldIfMissing(outerLocation, locationGroup, "institutionName");
        copyFieldIfMissing(outerLocation, locationGroup, "campusName");
        copyFieldIfMissing(outerLocation, locationGroup, "libraryName");
        copyFieldIfMissing(outerLocation, locationGroup, "name");
        copyFieldIfMissing(outerLocation, locationGroup, "locationName");
        copyFieldIfMissing(outerLocation, locationGroup, "code");
      } else {
        log.debug("Inner location exists but is not a JSON object: {}",
            nestedLocationObj != null ? nestedLocationObj.toString() : "null");
        locationGroup = outerLocation;
      }
    } else {
      locationGroup = outerLocation;
    }

    JsonObject callNumberGroup = itemData.getJsonObject(CALL_NUMBER);

    log.debug("Location Group for subfields: {}",
        locationGroup != null ? locationGroup.encodePrettily() : "null");

    // Even if locationGroup is empty, we try to process it to ensure the inactive
    // prefix is applied
    addSubFieldGroup(effectiveLocationSubFields, locationGroup, EffectiveLocationSubFields.getLocationValues());
    addSubFieldGroup(effectiveLocationSubFields, callNumberGroup, EffectiveLocationSubFields.getCallNumberValues());
    addSubFieldGroup(effectiveLocationSubFields, itemData, EffectiveLocationSubFields.getSimpleValues());

    updateSubfieldsMapWithItemLoanTypeSubfield(effectiveLocationSubFields, itemData);
    addLocationDiscoveryDisplayNameOrLocationNameSubfield(itemData, effectiveLocationSubFields);
    addLocationNameSubfield(itemData, effectiveLocationSubFields);

    // Special handling for locationName/LOCATION_NAME_SUBFIELD_CODE - ensure it has
    // data
    if (outerLocation != null && !isLocationActive) {
      // If we have a name in the outer location but no locationName in the effective
      // location subfields,
      // use the outer name as a fallback for the locationName (s) subfield
      String name = outerLocation.getString("name");
      if (!effectiveLocationSubFields.containsKey(LOCATION_NAME_SUBFIELD_CODE) && StringUtils.isNotBlank(name)) {
        effectiveLocationSubFields.put(LOCATION_NAME_SUBFIELD_CODE, "Inactive " + name);
        log.debug("Fallback: Added location name from outer location to subfield s: Inactive {}", name);
      }

      // Also ensure the display name subfield (d) has data for inactive locations
      if (!effectiveLocationSubFields.containsKey(LOCATION_DISCOVERY_DISPLAY_NAME_OR_LOCATION_NAME_SUBFIELD_CODE)
          && StringUtils.isNotBlank(name)) {
        effectiveLocationSubFields.put(LOCATION_DISCOVERY_DISPLAY_NAME_OR_LOCATION_NAME_SUBFIELD_CODE,
            "Inactive " + name);
        log.debug("Fallback: Added location display name from outer location to subfield d: Inactive {}", name);
      }

      // Ensure other location fields are also populated for inactive locations
      if (!effectiveLocationSubFields.containsKey("a")
          && StringUtils.isNotBlank(outerLocation.getString("institutionName"))) {
        effectiveLocationSubFields.put("a", "Inactive " + outerLocation.getString("institutionName"));
        log.debug("Fallback: Added institution name from outer location to subfield a: Inactive {}",
            outerLocation.getString("institutionName"));
      }

      if (!effectiveLocationSubFields.containsKey("b")
          && StringUtils.isNotBlank(outerLocation.getString("campusName"))) {
        effectiveLocationSubFields.put("b", "Inactive " + outerLocation.getString("campusName"));
        log.debug("Fallback: Added campus name from outer location to subfield b: Inactive {}",
            outerLocation.getString("campusName"));
      }

      if (!effectiveLocationSubFields.containsKey("c")
          && StringUtils.isNotBlank(outerLocation.getString("libraryName"))) {
        effectiveLocationSubFields.put("c", "Inactive " + outerLocation.getString("libraryName"));
        log.debug("Fallback: Added library name from outer location to subfield c: Inactive {}",
            outerLocation.getString("libraryName"));
      }
    }

    return effectiveLocationSubFields;
  }

  private void copyFieldIfMissing(JsonObject source, JsonObject target, String fieldName) {
    if (!target.containsKey(fieldName) && source.containsKey(fieldName)) {
      String value = source.getString(fieldName);
      if (StringUtils.isNotBlank(value)) {
        target.put(fieldName, value);
        log.debug("Copied field '{}' from outer location to nested location: {}", fieldName, value);
      }
    }
  }

  private void addLocationDiscoveryDisplayNameOrLocationNameSubfield(JsonObject itemData,
      Map<String, Object> effectiveLocationSubFields) {
    ofNullable(itemData.getJsonObject(LOCATION))
        .ifPresent(jo -> {
          String value = jo.getString(NAME);

          // More robust isActive determination
          Boolean isActiveValue = null;
          if (jo.containsKey("isActive")) {
            isActiveValue = jo.getBoolean("isActive");
          }

          boolean isActive;
          if (isActiveValue != null) {
            isActive = isActiveValue;
          } else {
            // Apply same heuristics as in main method
            JsonObject nestedLocation = jo.getJsonObject(LOCATION);
            boolean hasCompleteData = StringUtils.isNotBlank(value);

            if (nestedLocation != null && nestedLocation.isEmpty()) {
              isActive = false;
            } else if (!hasCompleteData) {
              isActive = false;
            } else {
              isActive = true;
            }
          }

          // If the nested location name is empty but we have an outer location name, use
          // that instead
          if (StringUtils.isBlank(value) && !isActive) {
            // Try to get the name from the outer location object if it exists
            JsonObject outerLocation = itemData.getJsonObject(LOCATION);
            if (outerLocation != null && outerLocation.containsKey(NAME)) {
              value = outerLocation.getString(NAME);
              log.debug("Using outer location name for inactive location display name subfield (d): {}", value);
            }
          }

          if (StringUtils.isNotBlank(value)) {
            if (!isActive) {
              value = "Inactive " + value;
              log.debug("Adding 'Inactive' prefix to location display name subfield (d): {}", value);
            }
            effectiveLocationSubFields.put(LOCATION_DISCOVERY_DISPLAY_NAME_OR_LOCATION_NAME_SUBFIELD_CODE, value);
          } else {
            log.debug("Location display name (d) is blank or missing in the location data: {}",
                jo != null ? jo.encodePrettily() : "null");
          }
        });
  }

  private void addLocationNameSubfield(JsonObject itemData, Map<String, Object> effectiveLocationSubFields) {
    JsonObject outerLocation = itemData.getJsonObject(LOCATION);
    if (outerLocation == null) {
      return;
    }

    // More robust isActive determination
    Boolean isActiveValue = null;
    if (outerLocation.containsKey("isActive")) {
      isActiveValue = outerLocation.getBoolean("isActive");
    }

    boolean isActive;
    if (isActiveValue != null) {
      isActive = isActiveValue;
    } else {
      // Apply same heuristics as in main method
      JsonObject nestedLocation = outerLocation.getJsonObject(LOCATION);
      boolean hasCompleteData = outerLocation.containsKey("name") &&
          StringUtils.isNotBlank(outerLocation.getString("name"));

      if (nestedLocation != null && nestedLocation.isEmpty()) {
        isActive = false;
      } else if (!hasCompleteData) {
        isActive = false;
      } else {
        isActive = true;
      }
    }

    // Try to get location name from nested location first
    JsonObject nestedLocation = outerLocation.getJsonObject(LOCATION);
    String value = null;

    if (nestedLocation != null) {
      value = nestedLocation.getString(LOCATION_NAME);
      if (StringUtils.isBlank(value)) {
        // Try alternative field names in nested location
        value = nestedLocation.getString(NAME);
      }
    }

    // If nested location is empty or doesn't have the value, and this is an
    // inactive location,
    // try to use the outer location's name as fallback
    if (StringUtils.isBlank(value) && !isActive) {
      value = outerLocation.getString(NAME);
      if (StringUtils.isNotBlank(value)) {
        log.debug("Using outer location name as fallback for location name subfield (s): {}", value);
      }
    }

    if (StringUtils.isNotBlank(value)) {
      if (!isActive) {
        value = "Inactive " + value;
        log.debug("Adding 'Inactive' prefix to location name subfield (s): {}", value);
      }
      effectiveLocationSubFields.put(LOCATION_NAME_SUBFIELD_CODE, value);
    } else {
      log.debug("Location name (s) is blank or missing in both nested and outer location data for {}active location",
          isActive ? "" : "in");
    }
  }

  private void updateSubfieldsMapWithItemLoanTypeSubfield(Map<String, Object> subFields, JsonObject itemData) {
    String permanentLoanType = itemData.getString(PERMANENT_LOAN_TYPE);
    String temporaryLoanType = itemData.getString(TEMPORARY_LOAN_TYPE);
    if (isNotEmpty(temporaryLoanType)) {
      subFields.put(LOAN_TYPE_SUBFIELD_CODE, temporaryLoanType);
      return;
    }
    if (isNotEmpty(permanentLoanType)) {
      subFields.put(LOAN_TYPE_SUBFIELD_CODE, permanentLoanType);
    }
  }

  private void addSubFieldGroup(Map<String, Object> effectiveLocationSubFields, JsonObject itemData,
      List<EffectiveLocationSubFields> subFieldGroupProperties) {
    if (nonNull(itemData)) {
      subFieldGroupProperties.forEach(pair -> {
        String subFieldCode = pair.getSubFieldCode();
        String subFieldValue = itemData.getString(pair.getJsonPropertyPath());

        // Check if this is a location field (institution, campus, library, location
        // name)
        // or if this is a location with isActive=false
        boolean isLocationField = EffectiveLocationSubFields.getLocationValues().contains(pair);

        // More robust inactive determination
        boolean isInactive = false;
        if (itemData.containsKey("isActive")) {
          isInactive = !itemData.getBoolean("isActive");
        } else if (isLocationField) {
          // For location fields, apply heuristics when isActive is missing
          JsonObject nestedLocation = itemData.getJsonObject(LOCATION);
          boolean hasCompleteData = itemData.containsKey("name") &&
              StringUtils.isNotBlank(itemData.getString("name"));

          if (nestedLocation != null && nestedLocation.isEmpty()) {
            isInactive = true;
          } else if (!hasCompleteData) {
            isInactive = true;
          }
        }

        if (isNotEmpty(subFieldValue)) {
          // Add "Inactive" prefix for any location field if the location is inactive
          if (isLocationField && isInactive) {
            subFieldValue = "Inactive " + subFieldValue;
            log.debug("Adding 'Inactive' prefix to {} subfield with value: {}", subFieldCode, subFieldValue);
          }

          effectiveLocationSubFields.put(subFieldCode, subFieldValue);
        } else if (isLocationField && isInactive) {
          // For inactive locations with empty nested data, try to get the value from the
          // parent location
          log.debug("Subfield {} is empty for inactive location, checking if we can use fallback data", subFieldCode);
          // This will be handled by the specific location subfield methods below
        } else {
          log.debug("Skipping subfield {} because value is empty or null. JSON path: {}",
              subFieldCode, pair.getJsonPropertyPath());
        }
      });
    } else {
      log.debug("Cannot add subfield group: itemData is null");
    }
  }

  private Map<String, Object> constructElectronicAccessSubFieldsMap(JsonObject itemData) {
    Map<String, Object> electronicAccessSubFields = new HashMap<>();
    Arrays.stream(ElectronicAccessSubFields.values())
        .forEach(pair -> {
          String subFieldCode = pair.getSubFieldCode();
          String subFieldValue = itemData.getString(pair.getJsonPropertyPath());
          if (isNotEmpty(subFieldValue)) {
            electronicAccessSubFields.put(subFieldCode, subFieldValue);
          }
        });
    return electronicAccessSubFields;
  }

  /**
   * Updates marc general info datafield(tag=999, ind1=ind2='f') with additional
   * subfield which holds data about record discovery
   * suppression status. Additional subfield has code = 't' and value = '0' if
   * record is discovery suppressed and '1' at opposite
   * case.
   *
   * @param metadataSource      - record source
   * @param metadataSourceOwner - record source owner
   * @return record source
   */
  public String updateMetadataSourceWithDiscoverySuppressedData(String metadataSource, JsonObject metadataSourceOwner) {
    JsonObject content = new JsonObject(metadataSource);
    JsonArray fields = content.getJsonArray(FIELDS);
    Optional<JsonObject> generalInfoDataFieldOptional = getGeneralInfoDataField(fields);
    generalInfoDataFieldOptional.ifPresent(jsonObject -> updateDataFieldWithDiscoverySuppressedData(jsonObject,
        metadataSourceOwner, GENERAL_INFO_FIELD_TAG_NUMBER));
    return content.encode();
  }

  /**
   * Updates marc electronic access field(tag=856) with additional subfield which
   * holds data about record discovery
   * suppression status. Additional subfield has code = 't' and value = '0' if
   * record is discovery suppressed and '1' at opposite
   * case.
   *
   * @param metadataSource      - record source
   * @param metadataSourceOwner - record source owner
   * @return record source
   */
  public String updateElectronicAccessFieldWithDiscoverySuppressedData(String metadataSource,
      JsonObject metadataSourceOwner) {
    JsonObject content = new JsonObject(metadataSource);
    JsonArray fields = content.getJsonArray(FIELDS);
    getElectronicAccessFields(fields).forEach(jsonObject -> updateDataFieldWithDiscoverySuppressedData(jsonObject,
        metadataSourceOwner, ELECTRONIC_ACCESS_FILED_TAG_NUMBER));
    return content.encode();
  }

  private Optional<JsonObject> getGeneralInfoDataField(JsonArray fields) {
    return fields.stream()
        .map(obj -> (JsonObject) obj)
        .filter(generalInfoFieldPredicate)
        .findFirst();
  }

  private List<JsonObject> getElectronicAccessFields(JsonArray fields) {
    return fields.stream()
        .map(obj -> (JsonObject) obj)
        .filter(electronicAccessPredicate)
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private void updateDataFieldWithDiscoverySuppressedData(JsonObject dataField,
      JsonObject sourceOwner,
      String tagNumber) {

    JsonObject dataFieldContent = dataField.getJsonObject(tagNumber);
    JsonArray subFields = dataFieldContent.getJsonArray(SUBFIELDS);
    List<Object> subFieldsList = subFields.getList();
    for (var s : subFieldsList) {
      var subField = (LinkedHashMap<String, Object>) s;
      if (subField.containsKey(SUPPRESS_DISCOVERY_CODE)) {
        return;
      }
    }
    Map<String, Object> discoverySuppressedSubField = new LinkedHashMap<>();
    int subFieldValue = storageHelper.getSuppressedFromDiscovery(sourceOwner) ? 1 : 0;
    discoverySuppressedSubField.put(DISCOVERY_SUPPRESSED_SUBFIELD_CODE, subFieldValue);
    subFieldsList.add(discoverySuppressedSubField);
  }

  public Predicate<JsonObject> getGeneralInfoFieldPredicate() {
    return generalInfoFieldPredicate;
  }

  public Predicate<JsonObject> getElectronicAccessPredicate() {
    return electronicAccessPredicate;
  }

  private enum EffectiveLocationSubFields {
    INSTITUTION_NAME("a", "institutionName"),
    CAMPUS_NAME("b", "campusName"),
    LIBRARY_NAME("c", "libraryName"),
    LOCATION_NAME("d", "name"),
    CALL_NUMBER("e", "callNumber"),
    CALL_NUMBER_PREFIX("f", "prefix"),
    CALL_NUMBER_SUFFIX("g", "suffix"),
    CALL_NUMBER_TYPE("h", "typeName"),
    MATERIAL_TYPE("i", "materialType"),
    VOLUME("j", "volume"),
    ENUMERATION("k", "enumeration"),
    CHRONOLOGY("l", "chronology"),
    BARCODE("m", "barcode"),
    COPY_NUMBER("n", "copyNumber");

    private String subFieldCode;
    private String jsonPropertyPath;

    EffectiveLocationSubFields(String subFieldCode, String jsonPropertyPath) {
      this.subFieldCode = subFieldCode;
      this.jsonPropertyPath = jsonPropertyPath;
    }

    public static List<EffectiveLocationSubFields> getLocationValues() {
      return Arrays.asList(INSTITUTION_NAME, CAMPUS_NAME, LIBRARY_NAME, LOCATION_NAME);
    }

    public static List<EffectiveLocationSubFields> getCallNumberValues() {
      return Arrays.asList(CALL_NUMBER, CALL_NUMBER_PREFIX, CALL_NUMBER_SUFFIX, CALL_NUMBER_TYPE);
    }

    public static List<EffectiveLocationSubFields> getSimpleValues() {
      return Arrays.asList(MATERIAL_TYPE, VOLUME, ENUMERATION, CHRONOLOGY, BARCODE, COPY_NUMBER);
    }

    public String getSubFieldCode() {
      return subFieldCode;
    }

    public String getJsonPropertyPath() {
      return jsonPropertyPath;
    }
  }

  private enum ElectronicAccessSubFields {
    URI("u", "uri"),
    LINK_TEXT("y", "linkText"),
    MATERIAL_TYPE("3", "materialsSpecification"),
    PUBLIC_NOTE("z", "publicNote");

    private String subFieldCode;
    private String jsonPropertyPath;

    ElectronicAccessSubFields(String subFieldCode, String jsonPropertyPath) {
      this.subFieldCode = subFieldCode;
      this.jsonPropertyPath = jsonPropertyPath;
    }

    public String getSubFieldCode() {
      return subFieldCode;
    }

    public String getJsonPropertyPath() {
      return jsonPropertyPath;
    }
  }

}
