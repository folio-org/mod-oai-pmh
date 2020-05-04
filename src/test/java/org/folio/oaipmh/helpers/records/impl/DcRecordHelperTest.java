package org.folio.oaipmh.helpers.records.impl;

import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.BiPredicate;

import javax.xml.bind.JAXBElement;

import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.ResponseHelper;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.folio.oaipmh.helpers.records.RecordHelper;
import org.folio.oaipmh.helpers.storage.SourceRecordStorageHelper;
import org.folio.oaipmh.helpers.storage.StorageHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openarchives.oai._2.MetadataType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2_0.oai_dc.Dc;
import org.purl.dc.elements._1.ElementType;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

class DcRecordHelperTest {

  private static final String RECORD_COLLECTION_PATH = File.separator + "record-helper";
  private static final String RECORD_COLLECTION_FILENAME = "records";

  private BiPredicate<JAXBElement<ElementType>, Boolean> suppressedDiscoveryFieldPredicate;
  private ConfigurationHelper configurationHelper = ConfigurationHelper.getInstance();
  private Collection<RecordType> recordCollection = new ArrayList<>();
  private RecordHelper recordHelper;
  private MetadataPrefix metadataPrefix;
  private StorageHelper storageHelper;

  @BeforeEach
  void setUp() {
    storageHelper = new SourceRecordStorageHelper();
    metadataPrefix = getMetadataPrefix();
    recordHelper = getRecordHelper();
    setupSuppressedDiscoveryFieldPredicate();
    JsonArray recordCollectionJson = configurationHelper
      .getJsonConfigFromResources(RECORD_COLLECTION_PATH, RECORD_COLLECTION_FILENAME)
      .getJsonArray("sourceRecords");
    recordCollectionJson.stream()
      .map(record -> (JsonObject) record)
      .forEach(jsonRecord -> {
        String source = storageHelper.getInstanceRecordSource(jsonRecord);
        RecordType record = new RecordType().withMetadata(buildOaiMetadata(source))
          .withSuppressDiscovery(storageHelper.getSuppressedFromDiscovery(jsonRecord));
        recordCollection.add(record);
      });
  }

  @Test
  void shouldUpdateCollection_whenSuppressedRecordsProcessingIsFalse() {
    System.setProperty(REPOSITORY_SUPPRESSED_RECORDS_PROCESSING, "false");

    recordHelper.updateRecordCollectionWithSuppressDiscoveryData(recordCollection);

    verifyRecordCollectionWasUpdated(recordCollection);
  }

  protected void verifyRecordCollectionWasUpdated(Collection<RecordType> collection) {
    collection.forEach(record -> {
      boolean discoverySuppressed = record.isSuppressDiscovery();
      Dc dc = (Dc) record.getMetadata()
        .getAny();
      boolean isRecordCollectionCorrect = dc.getTitlesAndCreatorsAndSubjects()
        .stream()
        .anyMatch(jaxbElement -> suppressedDiscoveryFieldPredicate.test(jaxbElement, discoverySuppressed));
      assertTrue(isRecordCollectionCorrect);
    });
  }

  private MetadataType buildOaiMetadata(String content) {
    MetadataType metadata = new MetadataType();
    byte[] byteSource = metadataPrefix.convert(content);
    Object record = ResponseHelper.getInstance()
      .bytesToObject(byteSource);
    metadata.setAny(record);
    return metadata;
  }

  protected RecordHelper getRecordHelper() {
    return new DcRecordHelper();
  }

  protected MetadataPrefix getMetadataPrefix() {
    return MetadataPrefix.DC;
  }

  private void setupSuppressedDiscoveryFieldPredicate() {
    suppressedDiscoveryFieldPredicate = (jaxbElement, discoverySuppressed) -> {
      String value = jaxbElement.getValue().getValue();
      return jaxbElement.getName().getLocalPart().equals("rights")
        && discoverySuppressed ? value.equals("discovery suppressed") : value.equals("discovery not suppressed");
    };
  }

}
