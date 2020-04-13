package org.folio.oaipmh.helpers;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.storage.StorageHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.folio.oaipmh.Constants.INVENTORY_STORAGE;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_STORAGE;
import static org.folio.oaipmh.Constants.SOURCE_RECORD_STORAGE;
import static org.folio.oaipmh.helpers.storage.InventoryStorageHelper.INSTANCES_URI;
import static org.folio.oaipmh.helpers.storage.SourceRecordStorageHelper.SOURCE_STORAGE_RESULT_URI;
import static org.folio.rest.impl.OkapiMockServer.EXIST_CONFIG_TENANT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableWithSize.iterableWithSize;
import static org.hamcrest.text.IsEmptyString.isEmptyOrNullString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class StorageHelperTest {
  private static final Logger logger = LoggerFactory.getLogger(StorageHelperTest.class);

  private static final String INSTANCE_ID = "00000000-0000-4000-a000-000000000000";

  @AfterEach
  void init() {
    System.clearProperty(REPOSITORY_STORAGE);
  }

  @Test
  void getInstanceWithException() {
    assertThrows(UnsupportedOperationException.class, () -> getStorageHelper(REPOSITORY_STORAGE));
  }

  @ParameterizedTest
  @ValueSource(strings = { SOURCE_RECORD_STORAGE, INVENTORY_STORAGE })
  void getItems(String storageType) {
    JsonObject entries = getJsonObjectFromFile(getDirPath(storageType) + "/instances_10.json");
    JsonArray items = getStorageHelper(storageType).getItems(entries);
    assertThat(items, is(notNullValue()));
    assertThat(items, is(iterableWithSize(10)));
  }

  @ParameterizedTest
  @ValueSource(strings = { SOURCE_RECORD_STORAGE, INVENTORY_STORAGE })
  void lastModifiedDate(String storageType) {
    JsonObject item = getJsonObjectFromFile(getDirPath(storageType) + "/instance.json");
    assertThat(getStorageHelper(storageType).getLastModifiedDate(item), is(notNullValue()));
  }

  @ParameterizedTest
  @ValueSource(strings = { SOURCE_RECORD_STORAGE, INVENTORY_STORAGE })
  void createdDate(String storageType) {
    JsonObject item = getJsonObjectFromFile(getDirPath(storageType) + "/instance_withCreatedDateOnly.json");
    assertThat(getStorageHelper(storageType).getLastModifiedDate(item), is(notNullValue()));
  }

  @ParameterizedTest
  @ValueSource(strings = { SOURCE_RECORD_STORAGE, INVENTORY_STORAGE })
  void epochDate(String storageType) {
    JsonObject item = getJsonObjectFromFile(getDirPath(storageType) + "/instance_withoutMetadata.json");
    assertThat(getStorageHelper(storageType).getLastModifiedDate(item), is(Instant.EPOCH));
  }

  @ParameterizedTest
  @ValueSource(strings = { SOURCE_RECORD_STORAGE, INVENTORY_STORAGE })
  void getItemId(String storageType) {
    JsonObject item = getJsonObjectFromFile(getDirPath(storageType) + "/instance.json");
    assertThat(getStorageHelper(storageType).getRecordId(item), not(isEmptyOrNullString()));
  }

  @Test
  void shouldReturnLinkedToRecordInstanceId_whenGetIdentifierAndStorageIsSRS(){
    JsonObject item = getJsonObjectFromFile(getDirPath(SOURCE_RECORD_STORAGE)+"/instance.json");
    assertEquals(INSTANCE_ID, getStorageHelper(SOURCE_RECORD_STORAGE).getIdentifierId(item));
  }

  @Test
  void shouldReturnEmptyString_whenGetIdentifierIdAndStorageIsSRSAndRecordHasNot999FieldWithLinkedInstanceId(){
    JsonObject item = getJsonObjectFromFile(getDirPath(SOURCE_RECORD_STORAGE)+"/instance_without999Field.json");
    assertEquals(StringUtils.EMPTY, getStorageHelper(SOURCE_RECORD_STORAGE).getIdentifierId(item));
  }

  @Test
  @ExtendWith(VertxExtension.class)
  void buildItemsEndpoint(Vertx vertx, VertxTestContext testContext) {
    vertx.runOnContext(event ->
      testContext.verify(() ->  {
        try {
          System.setProperty(REPOSITORY_MAX_RECORDS_PER_RESPONSE, "10");
          Map<String, String> okapiHeaders = new HashMap<>();
          okapiHeaders.put(OKAPI_TENANT, EXIST_CONFIG_TENANT);
          assertThat(getStorageHelper(SOURCE_RECORD_STORAGE).buildRecordsEndpoint(Request.builder().okapiHeaders(okapiHeaders).build()), is
            (equalTo(SOURCE_STORAGE_RESULT_URI + "?query=recordType%3D%3DMARC+and+additionalInfo.suppressDiscovery%3D%3Dfalse&limit=11&offset=0")));
          testContext.completeNow();
        } catch (UnsupportedEncodingException e) {
          testContext.failNow(e);
        } finally {
          System.clearProperty(REPOSITORY_MAX_RECORDS_PER_RESPONSE);
        }
      })
    );
  }

  private StorageHelper getStorageHelper(String storageType) {
    System.setProperty(REPOSITORY_STORAGE, storageType);
    return StorageHelper.getInstance();
  }

  private String getDirPath(String storageType) {
    return SOURCE_RECORD_STORAGE.equals(storageType) ? SOURCE_STORAGE_RESULT_URI : INSTANCES_URI;
  }

  /**
   * Creates {@link JsonObject} from the json file
   * @param path path to json file to read
   * @return {@link JsonObject} from the json file
   */
  private JsonObject getJsonObjectFromFile(String path) {
    try {
      File file = new File(StorageHelperTest.class.getResource(path).getFile());
      byte[] encoded = Files.readAllBytes(Paths.get(file.getPath()));
      return new JsonObject(new String(encoded, StandardCharsets.UTF_8));
    } catch (IOException e) {
      logger.error("Unexpected error", e);
      fail(e.getMessage());
    }
    return null;
  }
}
