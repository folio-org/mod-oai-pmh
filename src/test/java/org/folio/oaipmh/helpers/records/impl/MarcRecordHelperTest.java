package org.folio.oaipmh.helpers.records.impl;

import static org.folio.oaipmh.Constants.INSTANCE_SUPPRESS_FROM_DISCOVERY_SUBFIELD_CODE;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;

import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.helpers.records.RecordHelper;
import org.junit.jupiter.api.BeforeEach;
import org.openarchives.oai._2.RecordType;

import gov.loc.marc21.slim.DataFieldType;
import gov.loc.marc21.slim.SubfieldatafieldType;

class MarcRecordHelperTest extends DcRecordHelperTest {

  private BiPredicate<DataFieldType, Boolean> suppressedDiscoveryFieldPredicate;

  @BeforeEach
  void setUp() {
    super.setUp();
    setupSuppressedDiscoveryFieldPredicate();
  }

  @Override
  protected void verifyRecordCollectionWasUpdated(Collection<RecordType> collection) {
    collection.forEach(record -> {
      boolean discoverySuppressed = record.isSuppressDiscovery();
      gov.loc.marc21.slim.RecordType recordType = (gov.loc.marc21.slim.RecordType) record.getMetadata()
        .getAny();
      boolean isRecordCollectionCorrect = recordType.getDatafields()
        .stream()
        .anyMatch(dataField -> suppressedDiscoveryFieldPredicate.test(dataField, discoverySuppressed));
      assertTrue(isRecordCollectionCorrect);
    });
  }

  private void setupSuppressedDiscoveryFieldPredicate() {
    suppressedDiscoveryFieldPredicate = (dataField, discoverySuppressed) -> {
      List<SubfieldatafieldType> subfields = dataField.getSubfields();
      if (Objects.nonNull(subfields) && subfields.size() > 0) {
        return subfields.stream()
          .anyMatch(subfieldatafieldType -> {
            String value = subfieldatafieldType.getValue();
            return subfieldatafieldType.getCode()
              .equals(INSTANCE_SUPPRESS_FROM_DISCOVERY_SUBFIELD_CODE) && discoverySuppressed ? value.equals("1") : value.equals("0");
          });
      }
      return false;
    };
  }

  @Override
  protected RecordHelper getRecordHelper() {
    return new MarcRecordHelper();
  }

  @Override
  protected MetadataPrefix getMetadataPrefix() {
    return MetadataPrefix.MARC21XML;
  }
}
