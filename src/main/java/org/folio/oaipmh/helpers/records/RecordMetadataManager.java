package org.folio.oaipmh.helpers.records;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.CONTENT;
import static org.folio.oaipmh.Constants.FIELDS;
import static org.folio.oaipmh.Constants.FIRST_INDICATOR;
import static org.folio.oaipmh.Constants.GENERAL_INFO_FIELD_TAG_NUMBER;
import static org.folio.oaipmh.Constants.PARSED_RECORD;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.SECOND_INDICATOR;
import static org.folio.oaipmh.Constants.SUBFIELDS;
import static org.folio.oaipmh.Constants.SUPPRESS_FROM_DISCOVERY_SUBFIELD_CODE;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.storage.StorageHelper;

import com.google.common.collect.ImmutableMap;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Is used for manipulating with record metadata. Updates, constructs the new fields or already presented fields.
 */
public class RecordMetadataManager {

  private static final int FIRST_INDICATOR_INDEX = 0;
  private static final int SECOND_INDICATOR_INDEX = 1;
  private static final String ELECTRONIC_ACCESS_FILED_TAG_NUMBER = "856";
  private static final String EFFECTIVE_LOCATION_FILED_TAG_NUMBER = "952";
  private static final String INDICATOR_VALUE = "f";
  private static final String DISCOVERY_SUPPRESSED_SUBFIELD_CODE = "t";
  private static final String ITEMS = "items";
  private static final String ELECTRONIC_ACCESS = "electronicAccess";
  private static final String ITEMS_AND_HOLDINGS_FIELDS = "itemsandholdingsfields";
  private static final String LOCATION = "location";
  private static final String CALL_NUMBER = "callNumber";
  private static final String NAME = "name";

  private StorageHelper storageHelper = StorageHelper.getInstance();
  private final Map<String, String> indicatorsMap;
  private final Predicate<JsonObject> generalInfoFieldPredicate;
  private static RecordMetadataManager instance;

  private RecordMetadataManager() {
    indicatorsMap = new HashMap<>();
    indicatorsMap.put("No display constant generated", "4,8");
    indicatorsMap.put("", "4, ");
    indicatorsMap.put("Related resource", "4,2");
    indicatorsMap.put("Resource", "4,0");
    indicatorsMap.put("Version of resource", "4,1");

    generalInfoFieldPredicate = jsonObject -> {
      if (jsonObject.containsKey(GENERAL_INFO_FIELD_TAG_NUMBER)) {
        JsonObject dataFieldContent = jsonObject.getJsonObject(GENERAL_INFO_FIELD_TAG_NUMBER);
        String firstIndicator = dataFieldContent.getString(FIRST_INDICATOR);
        String secondIndicator = dataFieldContent.getString(SECOND_INDICATOR);
        return StringUtils.isNotEmpty(firstIndicator) && StringUtils.isNotEmpty(secondIndicator)
          && firstIndicator.equals(secondIndicator) && firstIndicator.equals(INDICATOR_VALUE);
      }
      return false;
    };
  }

  public static RecordMetadataManager getInstance() {
    if (Objects.nonNull(instance)) {
      return instance;
    }
    instance = new RecordMetadataManager();
    return instance;
  }

  /**
   * Updates metadata of retrieved from SRS record with related to it inventory items data.
   *
   * @param srsInstance       - record from SRS
   * @param inventoryInstance - instance form inventory storage
   */
  @SuppressWarnings("unchecked")
  public JsonObject populateMetadataWithItemsData(JsonObject srsInstance, JsonObject inventoryInstance) {
    JsonObject itemsAndHoldings;
    JsonArray items = null;
    try {
      itemsAndHoldings = inventoryInstance.getJsonObject(ITEMS_AND_HOLDINGS_FIELDS);
      items = itemsAndHoldings.getJsonArray(ITEMS);
    } catch (ClassCastException e) {
      //this means that inventory instance has no items and holdings
    }
    if (Objects.nonNull(items) && CollectionUtils.isNotEmpty(items.getList())) {
      JsonObject parsedRecord = srsInstance.getJsonObject(PARSED_RECORD);
      JsonObject content = parsedRecord.getJsonObject(CONTENT);
      JsonArray fields = content.getJsonArray(FIELDS);
      List<Object> fieldsList = fields.getList();
      items.forEach(item -> {
        updateFieldsWithItemEffectiveLocationField((JsonObject) item, fieldsList);
        updateFieldsWithItemElectronicAccessField((JsonObject) item, fieldsList);
      });
    }
    return srsInstance;
  }

  /**
   * Constructs field with subfields which is build from item location data. Constructed field has tag number = 952 and both
   * indicators has 'f' value.
   *
   * @param itemData         - json of single item
   * @param marcRecordFields - fields list to be updated with new one
   */
  private void updateFieldsWithItemEffectiveLocationField(JsonObject itemData, List<Object> marcRecordFields) {
    Map<String, Object> effectiveLocationSubFields = constructEffectiveLocationSubFieldsMap(itemData);
    FieldBuilder fieldBuilder = new FieldBuilder();
    Map<String, Object> effectiveLocationField = fieldBuilder.withFieldTagNumber(EFFECTIVE_LOCATION_FILED_TAG_NUMBER)
      .withFirstIndicator(INDICATOR_VALUE)
      .withSecondIndicator(INDICATOR_VALUE)
      .withSubFields(effectiveLocationSubFields)
      .build();
    marcRecordFields.add(effectiveLocationField);
  }

  /**
   * Constructs field with subfields which is build from item electronic access data. Constructed field has tag number = 856 and
   * both indicators depends on 'name' field of electronic access json (see {@link RecordMetadataManager#resolveIndicatorsValue}).
   *
   * @param itemData         - json of single item which contains array of electronic accesses
   * @param marcRecordFields - fields list to be updated with new one
   */
  private void updateFieldsWithItemElectronicAccessField(JsonObject itemData, List<Object> marcRecordFields) {
    JsonArray electronicAccessArray = itemData.getJsonArray(ELECTRONIC_ACCESS);
    if (Objects.nonNull(electronicAccessArray)) {
      electronicAccessArray.forEach(electronicAccess -> {
        Map<String, Object> electronicAccessSubFields = constructElectronicAccessSubFieldsMap((JsonObject) electronicAccess);
        FieldBuilder fieldBuilder = new FieldBuilder();
        List<String> indicators = resolveIndicatorsValue((JsonObject) electronicAccess);
        Map<String, Object> electronicAccessField = fieldBuilder.withFieldTagNumber(ELECTRONIC_ACCESS_FILED_TAG_NUMBER)
          .withFirstIndicator(indicators.get(FIRST_INDICATOR_INDEX))
          .withSecondIndicator(indicators.get(SECOND_INDICATOR_INDEX))
          .withSubFields(electronicAccessSubFields)
          .build();
        marcRecordFields.add(electronicAccessField);
      });
    }
  }

  private List<String> resolveIndicatorsValue(JsonObject electronicAccess) {
    String name = electronicAccess.getString(NAME);
    String key = StringUtils.isNotEmpty(name) ? name : EMPTY;
    String indicatorsInString = indicatorsMap.get(key);
    return Arrays.asList(indicatorsInString.split(","));
  }

  private Map<String, Object> constructEffectiveLocationSubFieldsMap(JsonObject itemData) {
    Map<String, Object> effectiveLocationSubFields = new HashMap<>();
    JsonObject locationGroup = itemData.getJsonObject(LOCATION)
      .getJsonObject(LOCATION);
    JsonObject callNumberGroup = itemData.getJsonObject(CALL_NUMBER);
    addSubFieldGroup(effectiveLocationSubFields, locationGroup, EffectiveLocationSubFields.getLocationValues());
    addSubFieldGroup(effectiveLocationSubFields, callNumberGroup, EffectiveLocationSubFields.getCallNumberValues());
    addSubFieldGroup(effectiveLocationSubFields, itemData, EffectiveLocationSubFields.getSimpleValues());
    return effectiveLocationSubFields;
  }

  private void addSubFieldGroup(Map<String, Object> effectiveLocationSubFields, JsonObject itemData,
      List<EffectiveLocationSubFields> subFieldGroupProperties) {
    subFieldGroupProperties.forEach(pair -> {
      String subFieldCode = pair.getSubFieldCode();
      String subFieldValue = itemData.getString(pair.getJsonPropertyPath());
      if (isNotEmpty(subFieldValue)) {
        effectiveLocationSubFields.put(subFieldCode, subFieldValue);
      }
    });
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
   * Updates marc general info datafield(tag=999, ind1=ind2='f') with additional subfield which holds data about record discovery
   * suppression status. Additional subfield has code = 't' and value = '0' if record is discovery suppressed and '1' at opposite
   * case.
   *
   * @param metadataSource      - record source
   * @param metadataSourceOwner - record source owner
   * @param request             - OAI-PMH request
   * @return record source
   */
  public String updateMetadataSourceWithDiscoverySuppressedDataIfNecessary(String metadataSource, JsonObject metadataSourceOwner,
      Request request) {
    if (getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING)) {
      JsonObject content = new JsonObject(metadataSource);
      JsonArray fields = content.getJsonArray(FIELDS);
      Optional<JsonObject> generalInfoDataFieldOptional = getGeneralInfoDataField(fields);
      if (generalInfoDataFieldOptional.isPresent()) {
        updateDataFieldWithDiscoverySuppressedData(generalInfoDataFieldOptional.get(), metadataSourceOwner);
      } else {
        appendGeneralInfoDatafieldWithDiscoverySuppressedData(fields, metadataSourceOwner);
      }
      return content.encode();
    }
    return metadataSource;
  }

  private Optional<JsonObject> getGeneralInfoDataField(JsonArray fields) {
    return fields.stream()
      .map(obj -> (JsonObject) obj)
      .filter(generalInfoFieldPredicate)
      .findFirst();
  }

  @SuppressWarnings("unchecked")
  private void updateDataFieldWithDiscoverySuppressedData(JsonObject generalInfoDataField, JsonObject sourceOwner) {
    JsonObject dataFieldContent = generalInfoDataField.getJsonObject(GENERAL_INFO_FIELD_TAG_NUMBER);
    JsonArray subFields = dataFieldContent.getJsonArray(SUBFIELDS);
    List<Object> subFieldsList = subFields.getList();

    Map<String, Object> discoverySuppressedSubField = new LinkedHashMap<>();
    int subFieldValue = storageHelper.getSuppressedFromDiscovery(sourceOwner) ? 1 : 0;
    discoverySuppressedSubField.put(DISCOVERY_SUPPRESSED_SUBFIELD_CODE, subFieldValue);
    subFieldsList.add(discoverySuppressedSubField);
  }

  @SuppressWarnings("unchecked")
  private void appendGeneralInfoDatafieldWithDiscoverySuppressedData(JsonArray fields, JsonObject sourceOwner) {
    List<Object> list = fields.getList();
    int subFieldValue = storageHelper.getSuppressedFromDiscovery(sourceOwner) ? 1 : 0;
    FieldBuilder fieldBuilder = new FieldBuilder();
    Map<String, Object> generalInfoDataField = fieldBuilder.withFieldTagNumber(GENERAL_INFO_FIELD_TAG_NUMBER)
      .withFirstIndicator(INDICATOR_VALUE)
      .withSecondIndicator(INDICATOR_VALUE)
      .withSubFields(ImmutableMap.of(SUPPRESS_FROM_DISCOVERY_SUBFIELD_CODE, subFieldValue))
      .build();
    list.add(generalInfoDataField);
  }

  public Predicate<JsonObject> getGeneralInfoFieldPredicate() {
    return generalInfoFieldPredicate;
  }

  private enum EffectiveLocationSubFields {
    INSTITUTION_NAME("a", "institutionName"),
    CAMPUS_NAME("b", "campusName"),
    LIBRARY_NAME("c", "libraryName"),
    LOCATION_NAME("d", "name"), // check default
    CALL_NUMBER("e", "callNumber"),
    CALL_NUMBER_PREFIX("f", "prefix"),
    CALL_NUMBER_SUFFIX("g", "suffix"),
    //this will work when inventory changes
    CALL_NUMBER_TYPE("h", "typeName"),
    MATERIAL_TYPE("i", "materialType"),
    VOLUME("j", "volume"),
    ENUMERATION("k", "enumeration"),
    CHRONOLOGY("l", "chronology"),
    BARCODE("m", "barcode");

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
      return Arrays.asList(MATERIAL_TYPE, VOLUME, ENUMERATION, CHRONOLOGY, BARCODE);
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
