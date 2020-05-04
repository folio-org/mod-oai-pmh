package org.folio.oaipmh.helpers.records.impl;

import static org.folio.oaipmh.Constants.GENERAL_INFO_DATA_FIELD_INDEX_VALUE;
import static org.folio.oaipmh.Constants.GENERAL_INFO_DATA_FIELD_TAG_NUMBER;
import static org.folio.oaipmh.Constants.INSTANCE_SUPPRESS_FROM_DISCOVERY_SUBFIELD_CODE;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import org.folio.oaipmh.helpers.records.RecordHelper;
import org.openarchives.oai._2.RecordType;

import gov.loc.marc21.slim.DataFieldType;
import gov.loc.marc21.slim.SubfieldatafieldType;

public class MarcRecordHelper implements RecordHelper {

  @Override
  public void updateRecordCollectionWithSuppressDiscoveryData(Collection<RecordType> records) {
    Predicate<DataFieldType> generalInfoDataFieldPredicate = dataFieldType ->
      dataFieldType.getInd1().equals(GENERAL_INFO_DATA_FIELD_INDEX_VALUE)
        && dataFieldType.getInd2().equals(GENERAL_INFO_DATA_FIELD_INDEX_VALUE)
        && dataFieldType.getTag().equals(GENERAL_INFO_DATA_FIELD_TAG_NUMBER);
    records.forEach(recordType -> {
      boolean isSuppressedFromDiscovery = recordType.isSuppressDiscovery();
      String suppressDiscoveryValue = isSuppressedFromDiscovery ? "1" : "0";
      gov.loc.marc21.slim.RecordType record = (gov.loc.marc21.slim.RecordType) recordType.getMetadata().getAny();
      List<DataFieldType> datafields = record.getDatafields();
      boolean alreadyContainsFolioSpecificDataField = datafields.stream()
        .anyMatch(generalInfoDataFieldPredicate);

      if(alreadyContainsFolioSpecificDataField) {
        updateGeneralInfoDataFieldWithSuppressDiscoverySubfield(generalInfoDataFieldPredicate, suppressDiscoveryValue, datafields);
      } else {
        buildGeneralInfoDataFieldWithSuppressDiscoverySubfield(suppressDiscoveryValue, datafields);
      }
    });
  }

  /**
   * Updates instance general info data field (marked with "999" tag and both indexes have "f" value) with new subfield which
   * holds data about record "suppress from discovery" state.
   *
   * @param generalInfoDataFieldPredicate - predicate with required field search criteria
   * @param suppressDiscoveryValue - value that has to be assigned to subfield value field
   * @param datafields - list of {@link DataFieldType} which contains folio specific data field
   */
  private void updateGeneralInfoDataFieldWithSuppressDiscoverySubfield(final Predicate<DataFieldType> generalInfoDataFieldPredicate, final String suppressDiscoveryValue, final List<DataFieldType> datafields) {
    datafields.stream()
      .filter(generalInfoDataFieldPredicate)
      .findFirst()
      .ifPresent(dataFieldType -> {
        List<SubfieldatafieldType> subfields = dataFieldType.getSubfields();
        subfields.add(new SubfieldatafieldType().withCode(INSTANCE_SUPPRESS_FROM_DISCOVERY_SUBFIELD_CODE)
          .withValue(suppressDiscoveryValue));
      });
  }

  /**
   * Builds instance general info data field (marked with "999" tag and both indexes have "f" value) with subfield which
   * holds data about record "suppress from discovery" state.
   *
   * @param suppressDiscoveryValue - value that has to be assigned to subfield value field
   * @param datafields - list of {@link DataFieldType} which contains folio specific data field
   */
  private void buildGeneralInfoDataFieldWithSuppressDiscoverySubfield(final String suppressDiscoveryValue, final List<DataFieldType> datafields) {
    DataFieldType generalInfoDataField = new DataFieldType();
    generalInfoDataField.setInd1(GENERAL_INFO_DATA_FIELD_INDEX_VALUE);
    generalInfoDataField.setInd2(GENERAL_INFO_DATA_FIELD_INDEX_VALUE);
    generalInfoDataField.setTag(GENERAL_INFO_DATA_FIELD_TAG_NUMBER);
    generalInfoDataField.withSubfields(new SubfieldatafieldType().withCode(INSTANCE_SUPPRESS_FROM_DISCOVERY_SUBFIELD_CODE)
      .withValue(suppressDiscoveryValue));
    datafields.add(generalInfoDataField);
  }
}
