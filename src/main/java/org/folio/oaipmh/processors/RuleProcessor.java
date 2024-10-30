package org.folio.oaipmh.processors;

import java.util.List;
import java.util.ArrayList;

import org.folio.processor.error.ErrorHandler;
import org.folio.processor.error.RecordInfo;
import org.folio.processor.error.TranslationException;
import org.folio.processor.referencedata.ReferenceDataWrapper;
import org.folio.processor.rule.Metadata;
import org.folio.processor.rule.Rule;
import org.folio.processor.translations.Translation;
import org.folio.processor.translations.TranslationFunction;
import org.folio.processor.translations.TranslationHolder;
import org.folio.processor.translations.TranslationsFunctionHolder;
import org.folio.reader.EntityReader;
import org.folio.reader.values.CompositeValue;
import org.folio.reader.values.ListValue;
import org.folio.reader.values.RuleValue;
import org.folio.reader.values.SimpleValue;
import org.folio.reader.values.StringValue;
import org.folio.writer.RecordWriter;
import org.marc4j.marc.VariableField;

import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.reader.values.SimpleValue.SubType.LIST_OF_STRING;
import static org.folio.reader.values.SimpleValue.SubType.STRING;

/**
 * RuleProcessor is a central part of mapping.
 * <p>
 * High-level algorithm:
 * # read data by the given rule
 * # translate data
 * # write data
 *
 * @see EntityReader
 * @see TranslationFunction
 * @see RecordWriter
 * @see Rule
 */
public final class RuleProcessor {
  private static final String LEADER_FIELD = "leader";
  private static final String EXCEPT_VALUES_IN_BRACKETS_REGEX = "\\[.*?]";
  private static final String INSTANCE_REGEX = "\\$.instance.";
  private static final String HOLDING_REGEX = "\\$.holdings.";
  private static final String ITEM_REGEX = "\\$.holdings.items.";

  private TranslationHolder translationHolder;

  private final List<TranslationException> usedTranslationExceptions = new ArrayList<>();

  public RuleProcessor() {
    this.translationHolder = TranslationsFunctionHolder.SET_VALUE;
  }

  public RuleProcessor(TranslationHolder translationHolder) {
    this.translationHolder = translationHolder;
  }

  /**
   * Reads and translates data by given rules, writes a marc record in specific format defined by RecordWriter.
   * Returns content of the generated marc record
   *
   * @return content of the generated marc record
   */
  public String process(EntityReader reader, RecordWriter writer, ReferenceDataWrapper referenceData, List<Rule> rules, ErrorHandler errorHandler) {
    rules.forEach(rule -> {
      if (LEADER_FIELD.equals(rule.getField())) {
        rule.getDataSources().forEach(dataSource -> writer.writeLeader(dataSource.getTranslation(), rule.getMetadata()));
      } else {
        processRule(reader, writer, referenceData, rule, errorHandler);
      }
    });
    usedTranslationExceptions.clear();
    return writer.getResult();
  }

  /**
   * Reads and translates data by given rules, writes a list of marc record fields in specific format defined by RecordWriter.
   * Returns the list of the generated VariableField of marc record
   *
   * @return the list of the generated VariableField of marc record
   */
  public List<VariableField> processFields(EntityReader reader, RecordWriter writer, ReferenceDataWrapper referenceData, List<Rule> rules, ErrorHandler errorHandler) {
    rules.forEach(rule -> {
      if (LEADER_FIELD.equals(rule.getField())) {
        rule.getDataSources().forEach(dataSource -> writer.writeLeader(dataSource.getTranslation(), rule.getMetadata()));
      } else {
        processRule(reader, writer, referenceData, rule, errorHandler);
      }
    });
    return writer.getFields();
  }

  /**
   * Processes the given mapping rule using reader and writer
   */
  private void processRule(EntityReader reader, RecordWriter writer, ReferenceDataWrapper referenceData, Rule rule, ErrorHandler errorHandler) {
    RuleValue<?> ruleValue = reader.read(rule);
    switch (ruleValue.getType()) {
      case SIMPLE:
        SimpleValue<?> simpleValue = (SimpleValue) ruleValue;
        translate(simpleValue, referenceData, rule.getMetadata(), errorHandler);
        writer.writeField(rule.getField(), simpleValue);
        break;
      case COMPOSITE:
        CompositeValue compositeValue = (CompositeValue) ruleValue;
        translate(compositeValue, referenceData, rule.getMetadata(), errorHandler);
        writer.writeField(rule.getField(), compositeValue);
        break;
      case MISSING:
    }
  }

  /**
   * Translates the given simple value
   */
  private <S extends SimpleValue> void translate(S simpleValue, ReferenceDataWrapper referenceData, Metadata metadata, ErrorHandler errorHandler) {
    if (translationHolder != null) {
      if (STRING.equals(simpleValue.getSubType())) {
        applyTranslation((StringValue) simpleValue, referenceData, metadata, 0, errorHandler);
      } else if (LIST_OF_STRING.equals(simpleValue.getSubType())) {
        applyTranslation((ListValue) simpleValue, referenceData, metadata, errorHandler);
      }
    }
  }

  /**
   * Translates the given composite value
   */
  private void translate(CompositeValue compositeValue, ReferenceDataWrapper referenceData, Metadata metadata, ErrorHandler errorHandler) {
    if (translationHolder != null) {
      List<List<StringValue>> value = compositeValue.getValue();
      for (int currentIndex = 0; currentIndex < value.size(); currentIndex++) {
        List<StringValue> readEntry = value.get(currentIndex);
        for (StringValue stringValue : readEntry) {
          applyTranslation(stringValue, referenceData, metadata, currentIndex, errorHandler);
        }
      }
    }
  }

  /**
   *  Applies translation function for ListValue
   */
  private void applyTranslation(ListValue listValue, ReferenceDataWrapper referenceData, Metadata metadata, ErrorHandler errorHandler) {
    Translation translation = listValue.getDataSource().getTranslation();
    if (translation != null) {
      for (int currentIndex = 0; currentIndex < listValue.getValue().size(); currentIndex++) {
        StringValue stringValue = listValue.getValue().get(currentIndex);
        String readValue = stringValue.getValue();
        RecordInfo recordInfo = stringValue.getRecordInfo();
        try {
          TranslationFunction translationFunction = translationHolder.lookup(translation.getFunction());
          String translatedValue = translationFunction.apply(readValue, currentIndex, translation, referenceData, metadata);
          stringValue.setValue(translatedValue);
        } catch (Exception e) {
          populateFieldNameAndValue(recordInfo, getProperFieldName(stringValue.getDataSource().getFrom(), recordInfo), readValue);
          handleError(recordInfo, e, errorHandler);
        }
      }
    }
  }

  /**
   *  Applies translation function for StringValue
   */
  private void applyTranslation(StringValue stringValue, ReferenceDataWrapper referenceData, Metadata metadata, int index, ErrorHandler errorHandler) {
    Translation translation = stringValue.getDataSource().getTranslation();
    if (translation != null) {
      String readValue = stringValue.getValue();
      RecordInfo recordInfo = stringValue.getRecordInfo();
      try {
        TranslationFunction translationFunction = translationHolder.lookup(translation.getFunction());
        String translatedValue = translationFunction.apply(readValue, index, translation, referenceData, metadata);
        stringValue.setValue(translatedValue);
      } catch (Exception e) {
        populateFieldNameAndValue(recordInfo, getProperFieldName(stringValue.getDataSource().getFrom(), recordInfo), readValue);
        handleError(recordInfo, e, errorHandler);
      }
    }
  }

  private void populateFieldNameAndValue(RecordInfo recordInfo, String fieldName, String fieldValue) {
    if (recordInfo != null) {
      recordInfo.setFieldName(fieldName);
      recordInfo.setFieldValue(fieldValue);
    }
  }

  private String getProperFieldName(String from, RecordInfo recordInfo) {
    if (recordInfo != null) {
      String nameWithoutDataFromBracket = from.replaceAll(EXCEPT_VALUES_IN_BRACKETS_REGEX, EMPTY);
      if (recordInfo.getType().isInstance()) {
        return nameWithoutDataFromBracket.replaceAll(INSTANCE_REGEX, EMPTY);
      }
      return recordInfo.getType().isHolding()
        ? nameWithoutDataFromBracket.replaceAll(HOLDING_REGEX, EMPTY)
        : nameWithoutDataFromBracket.replaceAll(ITEM_REGEX, EMPTY);
    }
    return EMPTY;
  }

  private void handleError(RecordInfo recordInfo, Exception e, ErrorHandler errorHandler) {
    TranslationException translationException = new TranslationException(recordInfo, e);
    if (!wasExceptionAlreadyThrown(translationException)) {
      usedTranslationExceptions.add(translationException);
      errorHandler.handle(translationException);
    }
  }

  private boolean wasExceptionAlreadyThrown(TranslationException transExc1) {
    return usedTranslationExceptions.stream()
      .anyMatch(transExc2 -> exceptionsEqual(transExc1, transExc2));
  }

  private boolean exceptionsEqual(TranslationException transExc1, TranslationException transExc2) {
    return transExc1.getErrorCode() == transExc2.getErrorCode()
      && ofNullable(transExc1.getRecordInfo().getId()).orElse("").equals(ofNullable(transExc2.getRecordInfo().getId()).orElse(""))
      && ofNullable(transExc1.getRecordInfo().getFieldValue()).orElse("").equals(ofNullable(transExc2.getRecordInfo().getFieldValue()).orElse(""))
      && ofNullable(transExc1.getRecordInfo().getFieldName()).orElse("").equals(ofNullable(transExc2.getRecordInfo().getFieldName()).orElse(""))
      && transExc1.getRecordInfo().getType() == transExc2.getRecordInfo().getType();
  }

}
