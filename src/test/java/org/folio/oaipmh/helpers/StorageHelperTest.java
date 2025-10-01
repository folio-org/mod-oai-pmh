package org.folio.oaipmh.helpers;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.oaipmh.Constants.REPOSITORY_STORAGE;
import static org.folio.oaipmh.Constants.SOURCE_RECORD_STORAGE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableWithSize.iterableWithSize;
import static org.hamcrest.text.IsEmptyString.isEmptyOrNullString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.helpers.storage.StorageHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StorageHelperTest {
  private static final Logger logger = LogManager.getLogger(StorageHelperTest.class);

  private static final String INSTANCE_ID = "00000000-0000-4000-a000-000000000000";

  private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  private static final String SOURCE_STORAGE_RECORD_PATH = "/source-storage/source-records";
  private static final String STORAGE_RECORD_PATH = "/source-storage/records";

  @Test
  void getInstanceWithException() {
    assertThrows(UnsupportedOperationException.class, () -> getStorageHelper(REPOSITORY_STORAGE));
  }

  @Test
  void getItems() {
    JsonObject entries = getJsonObjectFromFile(SOURCE_STORAGE_RECORD_PATH
        + "/instances_10_totalRecords_10.json");
    JsonArray items = getStorageHelper(SOURCE_RECORD_STORAGE).getItems(entries);
    assertThat(items, is(notNullValue()));
    assertThat(items, is(iterableWithSize(10)));
  }

  @Test
  void lastModifiedDate() {
    JsonObject item = getJsonObjectFromFile(SOURCE_STORAGE_RECORD_PATH + "/instance.json");
    assertThat(getStorageHelper(SOURCE_RECORD_STORAGE).getLastModifiedDate(item),
        is(notNullValue()));
  }

  @Test
  void createdDate() {
    JsonObject item = getJsonObjectFromFile(SOURCE_STORAGE_RECORD_PATH
        + "/instance_withCreatedDateOnly.json");
    assertThat(getStorageHelper(SOURCE_RECORD_STORAGE).getLastModifiedDate(item),
        is(notNullValue()));
  }

  @Test
  void epochDate() {
    JsonObject item = getJsonObjectFromFile(SOURCE_STORAGE_RECORD_PATH
        + "/instance_withoutMetadata.json");
    assertThat(getStorageHelper(SOURCE_RECORD_STORAGE).getLastModifiedDate(item),
        is(Instant.EPOCH));
  }

  @Test
  void getItemId() {
    JsonObject item = getJsonObjectFromFile(SOURCE_STORAGE_RECORD_PATH
        + "/instance.json");
    assertThat(getStorageHelper(SOURCE_RECORD_STORAGE).getRecordId(item),
        not(isEmptyOrNullString()));
  }

  @Test
  void getSuppressedFromDiscovery() {
    JsonObject item = getJsonObjectFromFile(SOURCE_STORAGE_RECORD_PATH + "/instance.json");
    assertFalse(getStorageHelper(SOURCE_RECORD_STORAGE).getSuppressedFromDiscovery(item));
  }

  @Test
  void getRecordSource() {
    JsonObject item = getJsonObjectFromFile(SOURCE_STORAGE_RECORD_PATH + "/instance.json");
    String recordSource = getStorageHelper(SOURCE_RECORD_STORAGE).getRecordSource(item);
    assertThat(recordSource, is(notNullValue()));
  }

  @Test
  void shouldReturnLinkedToRecordInstanceIdWhenGetIdentifierAndStorageIsSrs() {
    JsonObject item = getJsonObjectFromFile(SOURCE_STORAGE_RECORD_PATH + "/instance.json");
    assertEquals(INSTANCE_ID, getStorageHelper(SOURCE_RECORD_STORAGE).getIdentifierId(item));
  }

  @Test
  void shouldReturnEmptyStringWhenGetIdentifierIdAndStorageIsSrsAndRecordHasNotExternalIdsHolderField() {
    JsonObject item = getJsonObjectFromFile(SOURCE_STORAGE_RECORD_PATH
        + "/instance_withoutExternalIdsHolderField.json");
    assertEquals(EMPTY, getStorageHelper(SOURCE_RECORD_STORAGE).getIdentifierId(item));
  }


  @Test
  void getId() {
    JsonObject item = getJsonObjectFromFile(STORAGE_RECORD_PATH + "/instance.json");
    assertThat(getStorageHelper(SOURCE_RECORD_STORAGE).getId(item), not(isEmptyOrNullString()));
  }

  @Test
  void getRecordsItems() {
    JsonObject entries = getJsonObjectFromFile(STORAGE_RECORD_PATH
        + "/marc-e567b8e2-a45b-45f1-a85a-6b6312bdf4d8.json");
    JsonArray items = getStorageHelper(SOURCE_RECORD_STORAGE).getRecordsItems(entries);
    assertThat(items, is(notNullValue()));
    assertThat(items, is(iterableWithSize(1)));
  }

  @ParameterizedTest
  @ValueSource(chars = { 'a', 'c', 'd', 'n', 'p', 's' })
  void shouldMarkRecordAsDeletedOnlyWhenLeader05IsD(Character character) {
    var leader = new StringBuilder("12345ccm a2200361   4500");
    leader.setCharAt(5, character);
    JsonObject entry = new JsonObject(String.format("{ \"parsedRecord\": { \"content\": { "
        + "\"leader\": \"%s\" } }, \"deleted\": false }", leader));
    var isDeleted = getStorageHelper(SOURCE_RECORD_STORAGE).isRecordMarkAsDeleted(entry);
    assertThat(isDeleted, is(character.equals('d')));
  }

  private StorageHelper getStorageHelper(String storageType) {
    System.setProperty(REPOSITORY_STORAGE, storageType);
    return StorageHelper.getInstance();
  }

  /**
   * Creates {@link JsonObject} from the json file.
   *
   * @param path path to json file to read
   * @return {@link JsonObject} from the json file
   */
  private JsonObject getJsonObjectFromFile(String path) {
    try {
      File file = new File(StorageHelperTest.class.getResource(path).getFile());
      byte[] encoded = Files.readAllBytes(Paths.get(file.getPath()));
      return new JsonObject(new String(encoded, StandardCharsets.UTF_8));
    } catch (IOException e) {
      logger.error("Unexpected error.", e);
      fail(e.getMessage());
    }
    return null;
  }

}
