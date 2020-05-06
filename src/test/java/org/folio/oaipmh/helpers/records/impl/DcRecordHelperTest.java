package org.folio.oaipmh.helpers.records.impl;

import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.function.BiPredicate;

import javax.xml.bind.JAXBElement;

import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.ResponseHelper;
import org.folio.oaipmh.helpers.records.RecordHelper;
import org.folio.oaipmh.helpers.storage.SourceRecordStorageHelper;
import org.folio.oaipmh.helpers.storage.StorageHelper;
import org.folio.rest.impl.OkapiMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openarchives.oai._2.MetadataType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2_0.oai_dc.Dc;
import org.purl.dc.elements._1.ElementType;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

class DcRecordHelperTest {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private static final String RECORD_COLLECTION_PATH = "/record-helper/records.json";

  private BiPredicate<JAXBElement<ElementType>, Boolean> suppressedDiscoveryFieldPredicate;
  private Collection<RecordType> recordCollection = new ArrayList<>();
  private RecordHelper recordHelper;
  private MetadataPrefix metadataPrefix;
  private StorageHelper storageHelper;

  @BeforeEach
  void setUp() {
    storageHelper = new SourceRecordStorageHelper();
    metadataPrefix = getMetadataPrefix();
    recordHelper = RecordHelper.getInstance(metadataPrefix);
    setupSuppressedDiscoveryFieldPredicate();
    JsonArray recordCollectionJson = new JsonObject(Objects.requireNonNull(getJsonObjectFromFile(RECORD_COLLECTION_PATH)))
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
      boolean isRecordCorrect = dc.getTitlesAndCreatorsAndSubjects()
        .stream()
        .anyMatch(jaxbElement -> suppressedDiscoveryFieldPredicate.test(jaxbElement, discoverySuppressed));
      assertTrue(isRecordCorrect);
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

  private String getJsonObjectFromFile(String path) {
    try {
      logger.debug("Loading file " + path);
      URL resource = OkapiMockServer.class.getResource(path);
      if (resource == null) {
        return null;
      }
      File file = new File(resource.getFile());
      byte[] encoded = Files.readAllBytes(Paths.get(file.getPath()));
      return new String(encoded, StandardCharsets.UTF_8);
    } catch (IOException e) {
      logger.error("Unexpected error", e);
      fail(e.getMessage());
    }
    return null;
  }

}
