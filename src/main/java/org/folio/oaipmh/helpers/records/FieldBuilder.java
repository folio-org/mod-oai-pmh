package org.folio.oaipmh.helpers.records;

import static org.folio.oaipmh.Constants.FIRST_INDICATOR;
import static org.folio.oaipmh.Constants.SECOND_INDICATOR;
import static org.folio.oaipmh.Constants.SUBFIELDS;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FieldBuilder {

  private Map<String, Object> indicators = new LinkedHashMap<>();
  private Map<String, Object> subFields = new LinkedHashMap<>();
  private String fieldTagNumber;

  public FieldBuilder withFieldTagNumber(String fieldTagNumber) {
    this.fieldTagNumber = fieldTagNumber;
    return this;
  }

  public FieldBuilder withFirstIndicator(Object indicatorValue) {
    indicators.put(FIRST_INDICATOR, indicatorValue);
    return this;
  }

  public FieldBuilder withSecondIndicator(Object indicatorValue) {
    indicators.put(SECOND_INDICATOR, indicatorValue);
    return this;
  }

  public FieldBuilder withSubFields(Map<String, Object> subFields) {
    this.subFields.putAll(subFields);
    return this;
  }

  public Map<String, Object> build() {
    Map<String, Object> fieldContent = new LinkedHashMap<>();
    List<Object> subfields = new ArrayList<>();

    subfields.add(this.subFields);
    indicators.forEach(fieldContent::put);
    fieldContent.put(SUBFIELDS, subfields);
    Map<String, Object> field = new LinkedHashMap<>();
    field.put(fieldTagNumber, fieldContent);
    return field;
  }

}
