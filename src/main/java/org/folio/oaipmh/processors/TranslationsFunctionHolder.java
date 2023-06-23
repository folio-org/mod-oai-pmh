package org.folio.oaipmh.processors;

import com.google.common.base.Splitter;
import net.minidev.json.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.folio.processor.error.RecordInfo;
import org.folio.processor.error.RecordType;
import org.folio.processor.error.TranslationException;
import org.folio.processor.referencedata.JsonObjectWrapper;
import org.folio.processor.referencedata.ReferenceDataWrapper;
import org.folio.processor.rule.Metadata;
import org.folio.processor.translations.Translation;
import org.folio.processor.translations.TranslationFunction;
import org.folio.processor.translations.TranslationHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.processor.referencedata.ReferenceDataConstants.ALTERNATIVE_TITLE_TYPES;
import static org.folio.processor.referencedata.ReferenceDataConstants.CALL_NUMBER_TYPES;
import static org.folio.processor.referencedata.ReferenceDataConstants.CONTRIBUTOR_NAME_TYPES;
import static org.folio.processor.referencedata.ReferenceDataConstants.ELECTRONIC_ACCESS_RELATIONSHIPS;
import static org.folio.processor.referencedata.ReferenceDataConstants.IDENTIFIER_TYPES;
import static org.folio.processor.referencedata.ReferenceDataConstants.INSTANCE_FORMATS;
import static org.folio.processor.referencedata.ReferenceDataConstants.INSTANCE_TYPES;
import static org.folio.processor.referencedata.ReferenceDataConstants.LOAN_TYPES;
import static org.folio.processor.referencedata.ReferenceDataConstants.LOCATIONS;
import static org.folio.processor.referencedata.ReferenceDataConstants.MATERIAL_TYPES;
import static org.folio.processor.referencedata.ReferenceDataConstants.MODE_OF_ISSUANCES;
import static org.folio.processor.referencedata.ReferenceDataConstants.NATURE_OF_CONTENT_TERMS;

public enum TranslationsFunctionHolder implements TranslationFunction, TranslationHolder {

  SET_VALUE() {
    @Override
    public String apply(String value, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      return translation.getParameter(VALUE);
    }
  },
  SET_NATURE_OF_CONTENT_TERM() {
    @Override
    public String apply(String id, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      JSONObject entry = convertToJson(id, referenceData, NATURE_OF_CONTENT_TERMS);
      if (entry.isEmpty()) {
        LOGGER.error("Nature of content term is not found by the given id: {}", id);
        RecordInfo recordInfo = new RecordInfo(id, RecordType.INSTANCE);
        recordInfo.setFieldValue(NATURE_OF_CONTENT_TERMS);
        recordInfo.setFieldValue(id);
        throw new TranslationException(recordInfo, new Exception("Nature of content term is not found by the given id: " + id));
      } else {
        return entry.getAsString(NAME);
      }
    }
  },
  SET_IDENTIFIER() {
    @Override
    public String apply(String identifierValue, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      Object metadataIdentifierTypeIds = metadata.getData().get(IDENTIFIER_TYPE_METADATA).getData();
      if (metadataIdentifierTypeIds != null) {
        List<Map<String, String>> identifierTypes = (List<Map<String, String>>) metadataIdentifierTypeIds;
        if (identifierTypes.size() > currentIndex) {
          Map<String, String> currentIdentifierType = identifierTypes.get(currentIndex);
          JSONObject identifierType = convertToJson(currentIdentifierType.get(IDENTIFIER_TYPE_ID_PARAM), referenceData, IDENTIFIER_TYPES);
          if (!identifierType.isEmpty() && identifierType.getAsString(NAME).equalsIgnoreCase(translation.getParameter("type"))) {
            return identifierValue;
          }
        }
      }
      return StringUtils.EMPTY;
    }
  },
  SET_RELATED_IDENTIFIER() {
    @Override
    public String apply(String identifierValue, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      Object metadataIdentifierTypeIds = metadata.getData().get(IDENTIFIER_TYPE_METADATA).getData();
      if (metadataIdentifierTypeIds != null) {
        List<Map<String, String>> identifierTypes = (List<Map<String, String>>) metadataIdentifierTypeIds;
        if (identifierTypes.size() > currentIndex) {
          Map<String, String> currentIdentifierType = identifierTypes.get(currentIndex);
          JSONObject currentIdentifierTypeReferenceData = convertToJson(currentIdentifierType.get(IDENTIFIER_TYPE_ID_PARAM), referenceData, IDENTIFIER_TYPES);
          List<String> relatedIdentifierTypes = Splitter.on(",").splitToList(translation.getParameter(RELATED_IDENTIFIER_TYPES_PARAM));
          for (String relatedIdentifierType : relatedIdentifierTypes) {
            if (currentIdentifierTypeReferenceData.getAsString(NAME).equalsIgnoreCase(relatedIdentifierType)) {
              String actualIdentifierTypeName = translation.getParameter(TYPE_PARAM);
              for (JsonObjectWrapper wrapper : referenceData.get(IDENTIFIER_TYPES).values()) {
                JSONObject referenceDataEntry = new JSONObject(wrapper == null ? Collections.emptyMap() : wrapper.getMap());
                if (referenceDataEntry.getAsString(NAME).equalsIgnoreCase(actualIdentifierTypeName)) {
                  for (Map<String, String> identifierType : identifierTypes) {
                    if (identifierType.get(IDENTIFIER_TYPE_ID_PARAM).equalsIgnoreCase(referenceDataEntry.getAsString(ID_PARAM))) {
                      return identifierType.get(VALUE_PARAM);
                    }
                  }
                }
              }
            }
          }
        }
      }
      return StringUtils.EMPTY;
    }
  },
  SET_CONTRIBUTOR() {
    @Override
    public String apply(String identifierValue, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      Object metadataContributorNameTypeIds = metadata.getData().get("contributorNameTypeId").getData();
      if (metadataContributorNameTypeIds != null) {
        List<String> contributorNameTypeIds = (List<String>) metadataContributorNameTypeIds;
        if (contributorNameTypeIds.size() > currentIndex) {
          String contributorNameTypeId = contributorNameTypeIds.get(currentIndex);
          JSONObject contributorNameType = convertToJson(contributorNameTypeId, referenceData, CONTRIBUTOR_NAME_TYPES);
          if (!contributorNameType.isEmpty() && contributorNameType.getAsString(NAME).equalsIgnoreCase(translation.getParameter("type"))) {
            return identifierValue;
          }
        }
      }
      return StringUtils.EMPTY;
    }
  },
  SET_ALTERNATIVE_TITLE() {
    @Override
    public String apply(String identifierValue, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      Object metadataAlternativeTitleTypesIds = metadata.getData().get("alternativeTitleTypeId").getData();
      if (metadataAlternativeTitleTypesIds != null) {
        List<String> alternativeTitleTypesIds = (List<String>) metadataAlternativeTitleTypesIds;
        if (alternativeTitleTypesIds.size() > currentIndex) {
          String alternativeTitleTypeId = alternativeTitleTypesIds.get(currentIndex);
          JSONObject alternativeTitleType = convertToJson(alternativeTitleTypeId, referenceData, ALTERNATIVE_TITLE_TYPES);
          if (!alternativeTitleType.isEmpty() && alternativeTitleType.getAsString(NAME).equalsIgnoreCase(translation.getParameter("type"))) {
            return identifierValue;
          }
        }
      }
      return StringUtils.EMPTY;
    }
  },

  SET_LOAN_TYPE() {
    @Override
    public String apply(String id, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      JSONObject entry = convertToJson(id, referenceData, LOAN_TYPES);
      if (entry.isEmpty()) {
        RecordInfo recordInfo = new RecordInfo(id, RecordType.ITEM);
        recordInfo.setFieldValue(LOAN_TYPES);
        recordInfo.setFieldValue(id);
        LOGGER.error("Loan Type is not found by the given id: {}", id);
        throw new TranslationException(recordInfo, new Exception("Loan type is not found by the given id: " + id));
      } else {
        return entry.getAsString(NAME);
      }
    }
  },

  SET_MATERIAL_TYPE() {
    @Override
    public String apply(String materialTypeId, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      JSONObject entry = convertToJson(materialTypeId, referenceData, MATERIAL_TYPES);
      if (entry.isEmpty()) {
        RecordInfo recordInfo = new RecordInfo(materialTypeId, RecordType.ITEM);
        recordInfo.setFieldValue(MATERIAL_TYPES);
        recordInfo.setFieldValue(materialTypeId);
        LOGGER.error("Material type is not found by the given id: {}", materialTypeId);
        throw new TranslationException(recordInfo, new Exception("Material type is not found by the given id: " + materialTypeId));
      } else {
        return entry.getAsString(NAME);
      }
    }
  },

  /**
   * Sixteen characters that indicate the date and time of the latest record transaction
   * and serve as a version identifier for the record.
   * They are recorded according to Representation of Dates and Times (ISO 8601).
   * The date requires 8 numeric characters in the pattern yyyymmdd.
   * The time requires 8 numeric characters in the pattern hhmmss.f, expressed in terms of the 24-hour (00-23) clock.
   */
  SET_TRANSACTION_DATETIME() {
    private transient DateTimeFormatter targetDateFormatter = new DateTimeFormatterBuilder()
      .appendPattern("yyyyMMddhhmmss")
      .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 1, true)
      .toFormatter();

    @Override
    public String apply(String updatedDate, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) throws ParseException {
      ZonedDateTime originDateTime = getParsedDate(updatedDate);
      return targetDateFormatter.format(originDateTime);
    }
  },

  /**
   * Forty character positions (00-39) that provide coded information about the record as a whole and about special
   * bibliographic aspects of the item being cataloged.
   * These coded data elements are potentially useful for retrieval and data management purposes.
   * Format:
   * 00-05 - Metadata.createdDate field in yymmdd format
   * 06 is set to | (pipe character)
   * 07-10 to publication[0] dateOfPublication if can be formatted else |||| (four pipe characters)
   * 11-14 to publication[1] dateOfPublication if can be formatted else |||| (four pipe characters)
   * 18-22 - each field set to |
   * 23-29 - each field to be blank
   * 30-34 - each field set to |
   * 35-37 - if languages array is empty set it to "und",
   * if one element, use it to populate the field (it should be 3 letter language code),
   * if the array contains more than one language, then set it to "mul"
   * 38-39 - each field set to |
   */
  SET_FIXED_LENGTH_DATA_ELEMENTS() {
    private transient DateTimeFormatter targetCreatedDateFormatter = DateTimeFormatter.ofPattern("yyMMdd");
    private static final String DATES_OF_PUBLICATION = "datesOfPublication";
    private static final String LANGUAGES = "languages";
    private static final String FIELD_PATTERN = "%s|%s%s||||||||       |||||%s||";

    @Override
    public String apply(String originCreatedDate, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      String createdDateParam;
      if (isNotEmpty(originCreatedDate)) {
        try {
          createdDateParam = targetCreatedDateFormatter.format(getParsedDate(originCreatedDate));
        } catch (ParseException e) {
          LOGGER.error("Failed to parse createdDate field, the current time value will be used");
          createdDateParam = targetCreatedDateFormatter.format(ZonedDateTime.now());
        }
      } else {
        createdDateParam = targetCreatedDateFormatter.format(ZonedDateTime.now());
      }

      String publicationDate0Param = "||||";
      String publicationDate1Param = "||||";
      if (metadata != null && metadata.getData().containsKey(DATES_OF_PUBLICATION)
        && metadata.getData().get(DATES_OF_PUBLICATION) != null) {
        List<String> publicationDates = (List<String>) metadata.getData().get(DATES_OF_PUBLICATION).getData();
        if (publicationDates.size() == 1 && isNotEmpty(publicationDates.get(0)) && publicationDates.get(0).length() == 4) {
          publicationDate0Param = publicationDates.get(0);
        } else if (publicationDates.size() > 1) {
          String publicationDate0 = publicationDates.get(0);
          if (isNotEmpty(publicationDate0) && publicationDate0.length() == 4) {
            publicationDate0Param = publicationDate0;
          }
          String publicationDate1 = publicationDates.get(1);
          if (isNotEmpty(publicationDate1) && publicationDate1.length() == 4) {
            publicationDate1Param = publicationDate1;
          }
        }
      }

      String languageParam = "und";
      if (metadata != null && metadata.getData().containsKey(LANGUAGES)
        && metadata.getData().get(LANGUAGES) != null) {
        List<String> languages = (List<String>) metadata.getData().get(LANGUAGES).getData();
        if (languages.size() == 1 && isNotEmpty(languages.get(0))) {
          languageParam = languages.get(0);
        } else if (languages.size() > 1) {
          languageParam = "mul";
        }
      }
      return format(FIELD_PATTERN, createdDateParam, publicationDate0Param, publicationDate1Param, languageParam);
    }
  },

  SET_INSTANCE_TYPE_ID() {
    @Override
    public String apply(String instanceTypeId, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      JSONObject entry = convertToJson(instanceTypeId, referenceData, INSTANCE_TYPES);
      if (entry.isEmpty()) {
        LOGGER.error("Instance type id is not found by the given id: {}", instanceTypeId);
        RecordInfo recordInfo = new RecordInfo(instanceTypeId, RecordType.INSTANCE);
        recordInfo.setFieldValue(INSTANCE_TYPES);
        recordInfo.setFieldValue(instanceTypeId);
        throw new TranslationException(recordInfo, new Exception("Instance type id is not found by the given id: " + instanceTypeId));
      } else {
        return entry.getAsString(NAME);
      }
    }
  },

  SET_INSTANCE_FORMAT_ID() {
    private static final String REGEX = "--";

    @Override
    public String apply(String instanceFormatId, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      JSONObject entry = convertToJson(instanceFormatId, referenceData, INSTANCE_FORMATS);
      if (entry.isEmpty()) {
        LOGGER.error("Instance format is not found by the given id: {}", instanceFormatId);
        RecordInfo recordInfo = new RecordInfo(instanceFormatId, RecordType.INSTANCE);
        recordInfo.setFieldValue(INSTANCE_FORMATS);
        recordInfo.setFieldValue(instanceFormatId);
        throw new TranslationException(recordInfo, new Exception("Instance format is not found by the given id: " + instanceFormatId));
      } else {
        String instanceFormatIdValue = entry.getAsString(NAME);
        String[] instanceFormatsResult = instanceFormatIdValue.split(REGEX);
        if (translation.getParameter(VALUE).equals("0") && isNotBlank(instanceFormatsResult[0])) {
          return instanceFormatsResult[0].trim();
        } else if (translation.getParameter(VALUE).equals("1") && instanceFormatsResult.length > 1 && isNotBlank(instanceFormatsResult[1])) {
          return instanceFormatsResult[1].trim();
        } else {
          return StringUtils.EMPTY;
        }
      }
    }
  },

  SET_ELECTRONIC_ACCESS_INDICATOR() {
    @Override
    public String apply(String value, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      List<String> relationshipIds = (List<String>) metadata.getData().get("relationshipId").getData();
      if (CollectionUtils.isNotEmpty(relationshipIds) && relationshipIds.size() > currentIndex) {
        String relationshipId = relationshipIds.get(currentIndex);
        JSONObject entry = convertToJson(relationshipId, referenceData, ELECTRONIC_ACCESS_RELATIONSHIPS);
        if (!entry.isEmpty()) {
          String relationshipName = entry.getAsString(NAME);
          for (Map.Entry<String, String> parameter : translation.getParameters().entrySet()) {
            if (relationshipName.equalsIgnoreCase(parameter.getKey())) {
              return parameter.getValue();
            }
          }
        }
      }
      return StringUtils.SPACE;
    }
  },

  SET_MODE_OF_ISSUANCE_ID() {
    @Override
    public String apply(String modeOfIssuanceId, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      JSONObject entry = convertToJson(modeOfIssuanceId, referenceData, MODE_OF_ISSUANCES);
      if (entry.isEmpty()) {
        LOGGER.error("Mode of issuance is not found by the given id: {}", modeOfIssuanceId);
        RecordInfo recordInfo = new RecordInfo(modeOfIssuanceId, RecordType.INSTANCE);
        recordInfo.setFieldValue(MODE_OF_ISSUANCES);
        recordInfo.setFieldValue(modeOfIssuanceId);
        throw new TranslationException(recordInfo, new Exception("Mode of issuance is not found by the given id: " + modeOfIssuanceId));
      } else {
        return entry.getAsString(NAME);
      }
    }
  },

  SET_CALL_NUMBER_TYPE_ID() {
    @Override
    public String apply(String typeId, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      JSONObject entry = convertToJson(typeId, referenceData, CALL_NUMBER_TYPES);
      if (entry.isEmpty()) {
        LOGGER.error("Call number type is not found by the given id: {}", typeId);
        RecordInfo recordInfo = new RecordInfo(typeId, RecordType.ITEM);
        recordInfo.setFieldValue(CALL_NUMBER_TYPES);
        recordInfo.setFieldValue(typeId);
        throw new TranslationException(recordInfo, new Exception("Call number type is not found by the given id: " + typeId));
      } else {
        return entry.getAsString(NAME);
      }
    }
  },

  SET_LOCATION() {
    @Override
    public String apply(String locationId, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) {
      JSONObject entry = convertToJson(locationId, referenceData, LOCATIONS);
      if (entry.isEmpty()) {
        LOGGER.error("Location is not found by the given id: {}", locationId);
        RecordInfo recordInfo = new RecordInfo(locationId, RecordType.INSTANCE);
        recordInfo.setFieldValue(LOCATIONS);
        recordInfo.setFieldValue(locationId);
        throw new TranslationException(recordInfo, new Exception("Location is not found by the given id: " + locationId));
      } else {
        String relatedReferenceData = translation.getParameter("referenceData");
        String referenceDataIdField = translation.getParameter("referenceDataIdField");
        String field = translation.getParameter("field");
        if (relatedReferenceData != null && referenceDataIdField != null && field != null) {
          String referenceDataIdValue = entry.getAsString(referenceDataIdField);
          JSONObject relatedEntry = convertToJson(referenceDataIdValue, referenceData, relatedReferenceData);
          if (relatedEntry.isEmpty()) {
            RecordInfo recordInfo = new RecordInfo(locationId, RecordType.INSTANCE);
            recordInfo.setFieldValue(LOCATIONS);
            recordInfo.setFieldValue(locationId);
            LOGGER.error("Data related for location is not found {} by the given id: {}", relatedReferenceData, referenceDataIdValue);
            throw new TranslationException(recordInfo, new Exception(String.format("Data related for location is not found %s by the given id: %s",
              relatedReferenceData, referenceDataIdValue)));
          } else {
            return relatedEntry.getAsString(field);
          }
        } else if (field != null) {
          return entry.getAsString(field);
        }
        return StringUtils.EMPTY;
      }
    }
  },

  SET_HOLDINGS_PERMANENT_LOCATION() {
    @Override
    public String apply(String locationId, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) throws ParseException {
      List<String> temporaryLocationId = (List<String>) metadata.getData().get("temporaryLocationId").getData();
      if (CollectionUtils.isNotEmpty(temporaryLocationId) && isNotEmpty(temporaryLocationId.get(0))) {
        return StringUtils.EMPTY;
      } else {
        JSONObject entry = convertToJson(locationId, referenceData, LOCATIONS);
        if (entry.isEmpty()) {
          LOGGER.error("Location is not found by the given id: {}", locationId);
          RecordInfo recordInfo = new RecordInfo(locationId, RecordType.HOLDING);
          recordInfo.setFieldValue(LOCATIONS);
          recordInfo.setFieldValue(locationId);
          throw new TranslationException(recordInfo, new Exception("Location is not found by the given id: " + locationId));
        } else {
          return entry.getAsString(NAME);
        }
      }
    }
  },

  SET_METADATA_DATE_TIME() {
    private transient DateTimeFormatter targetFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd:hh-mm-ss");

    @Override
    public String apply(String date, int currentIndex, Translation translation, ReferenceDataWrapper referenceData, Metadata metadata) throws ParseException {
      ZonedDateTime originDateTime = getParsedDate(date);
      return targetFormatter.format(originDateTime);
    }
  };

  private static JSONObject convertToJson(String id, ReferenceDataWrapper referenceData, String referenceDataKey) {
    JsonObjectWrapper wrapper = referenceData.get(referenceDataKey).get(id);
    return new JSONObject(wrapper == null ? Collections.emptyMap() : wrapper.getMap());
  }

  private static final String VALUE_PARAM = "value";
  private static final String ID_PARAM = "id";
  private static final String TYPE_PARAM = "type";
  private static final String RELATED_IDENTIFIER_TYPES_PARAM = "relatedIdentifierTypes";
  private static final String IDENTIFIER_TYPE_ID_PARAM = "identifierTypeId";
  private static final String IDENTIFIER_TYPE_METADATA = "identifierType";
  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String NAME = "name";
  private static final String VALUE = VALUE_PARAM;

  @Override
  public TranslationFunction lookup(String function) {
    return valueOf(function.toUpperCase());
  }

  private static ZonedDateTime getParsedDate(String incomingDate) throws ParseException {
    String[] patterns = {"yyyy-MM-dd'T'HH:mm:ss.SSSZ", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"};
    Date date = DateUtils.parseDateStrictly(incomingDate, patterns);
    return date.toInstant().atZone(ZoneId.of("Z"));
  }
}
