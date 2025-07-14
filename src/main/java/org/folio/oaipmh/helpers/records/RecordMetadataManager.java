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
    Map<String, Object> effectiveLocationSubFields = new HashMap<>();
    JsonObject locationGroup = null;
    if (nonNull(itemData.getJsonObject(LOCATION))) {
      locationGroup = itemData.getJsonObject(LOCATION).getJsonObject(LOCATION);
    }
    JsonObject callNumberGroup = itemData.getJsonObject(CALL_NUMBER);
    addSubFieldGroup(effectiveLocationSubFields, locationGroup, EffectiveLocationSubFields.getLocationValues());
    addSubFieldGroup(effectiveLocationSubFields, callNumberGroup, EffectiveLocationSubFields.getCallNumberValues());
    addSubFieldGroup(effectiveLocationSubFields, itemData, EffectiveLocationSubFields.getSimpleValues());
    updateSubfieldsMapWithItemLoanTypeSubfield(effectiveLocationSubFields, itemData);
    addLocationDiscoveryDisplayNameOrLocationNameSubfield(itemData, effectiveLocationSubFields);
    addLocationNameSubfield(itemData, effectiveLocationSubFields);
    return effectiveLocationSubFields;
  }

  private void addLocationDiscoveryDisplayNameOrLocationNameSubfield(JsonObject itemData,
      Map<String, Object> effectiveLocationSubFields) {
  ofNullable(itemData.getJsonObject(LOCATION))
      .ifPresent(loc -> {
        String displayName = loc.getString(NAME);
        JsonObject nestedLoc = loc.getJsonObject(LOCATION);

        // Check isActive at both levels - if either is false, consider location inactive
        boolean locIsActive = loc.getBoolean("isActive", true);
        boolean nestedLocIsActive = ofNullable(nestedLoc).map(n -> n.getBoolean("isActive", true)).orElse(true);
        boolean isActive = locIsActive && nestedLocIsActive;

        String finalValue = null;

        if (StringUtils.isNotBlank(displayName)) {
          finalValue = isActive ? displayName : "Inactive " + displayName;
        } else if (nestedLoc != null) {
          String fallbackName = nestedLoc.getString(LOCATION_NAME);
          if (StringUtils.isNotBlank(fallbackName)) {
            finalValue = isActive ? fallbackName : "Inactive " + fallbackName;
          }
        }

        if (finalValue != null) {
          effectiveLocationSubFields.put(LOCATION_DISCOVERY_DISPLAY_NAME_OR_LOCATION_NAME_SUBFIELD_CODE, finalValue);
        }
      });
}


  private void addLocationNameSubfield(JsonObject itemData, Map<String, Object> effectiveLocationSubFields) {
    ofNullable(itemData.getJsonObject(LOCATION))
        .ifPresent(loc -> {
          JsonObject nestedLoc = loc.getJsonObject(LOCATION);
          if (nestedLoc != null) {
            String locationName = nestedLoc.getString(LOCATION_NAME);
            if (StringUtils.isNotBlank(locationName)) {
              // Check isActive at both levels - if either is false, consider location inactive
              boolean locIsActive = loc.getBoolean("isActive", true);
              boolean nestedLocIsActive = nestedLoc.getBoolean("isActive", true);
              boolean isActive = locIsActive && nestedLocIsActive;
              
              String finalName = isActive ? locationName : "Inactive " + locationName;
              effectiveLocationSubFields.put(LOCATION_NAME_SUBFIELD_CODE, finalName);
            }
          }
        });
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
        if (isNotEmpty(subFieldValue)) {
          effectiveLocationSubFields.put(subFieldCode, subFieldValue);
        }
      });
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
      return Arrays.asList(INSTITUTION_NAME, CAMPUS_NAME, LIBRARY_NAME);
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
