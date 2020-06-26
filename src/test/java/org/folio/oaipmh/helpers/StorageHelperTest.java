package org.folio.oaipmh.helpers;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.oaipmh.Constants.INVENTORY_STORAGE;
import static org.folio.oaipmh.Constants.ISO_UTC_DATE_ONLY;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_STORAGE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.SOURCE_RECORD_STORAGE;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class StorageHelperTest {
  private static final Logger logger = LoggerFactory.getLogger(StorageHelperTest.class);

  private static final String INSTANCE_ID = "00000000-0000-4000-a000-000000000000";

  private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  private static final String SOURCE_STORAGE_RECORD_PATH = "/source-storage/source-records";

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
    JsonObject entries = getJsonObjectFromFile(getDirPath(storageType) + "/instances_10_totalRecords_10.json");
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
  void getSuppressedFromDiscovery(String storageType) {
    JsonObject item = getJsonObjectFromFile(getDirPath(SOURCE_RECORD_STORAGE) + "/instance.json");
    assertFalse(getStorageHelper(storageType).getSuppressedFromDiscovery(item));
  }

  @ParameterizedTest
  @ValueSource(strings = { SOURCE_RECORD_STORAGE, INVENTORY_STORAGE })
  void getRecordSource(String storageType) {
    JsonObject item = getJsonObjectFromFile(getDirPath(storageType) + "/instance.json");
    String recordSource = getStorageHelper(storageType).getRecordSource(item);
    assertThat(recordSource, is(notNullValue()));
  }

  @Test
  void shouldReturnLinkedToRecordInstanceId_whenGetIdentifierAndStorageIsSRS(){
    JsonObject item = getJsonObjectFromFile(getDirPath(SOURCE_RECORD_STORAGE)+"/instance.json");
    assertEquals(INSTANCE_ID, getStorageHelper(SOURCE_RECORD_STORAGE).getIdentifierId(item));
  }

  @Test
  void shouldReturnEmptyString_whenGetIdentifierIdAndStorageIsSRSAndRecordHasNotExternalIdsHolderField(){
    JsonObject item = getJsonObjectFromFile(getDirPath(SOURCE_RECORD_STORAGE)+"/instance_withoutExternalIdsHolderField.json");
    assertEquals(EMPTY, getStorageHelper(SOURCE_RECORD_STORAGE).getIdentifierId(item));
  }

  @Test
  @ExtendWith(VertxExtension.class)
  void buildItemsEndpointWithSuppressDiscoveryTrue(Vertx vertx, VertxTestContext testContext) {
    vertx.runOnContext(event ->
      testContext.verify(() ->  {
        try {
          System.setProperty(REPOSITORY_MAX_RECORDS_PER_RESPONSE, "10");
          System.setProperty(REPOSITORY_SUPPRESSED_RECORDS_PROCESSING, "false");
          System.setProperty(REPOSITORY_DELETED_RECORDS, "no");
          Map<String, String> okapiHeaders = new HashMap<>();
          okapiHeaders.put(OKAPI_TENANT, EXIST_CONFIG_TENANT);
          String formattedCurrentDate = getFormattedCurrentDate();
          assertThat(getStorageHelper(SOURCE_RECORD_STORAGE).buildRecordsEndpoint(Request.builder().okapiHeaders(okapiHeaders).build(), false), is
            (equalTo(SOURCE_STORAGE_RESULT_URI + "?query=recordType%3D%3DMARC+and+additionalInfo.suppressDiscovery%3D%3Dfalse"
              + "+and+metadata.updatedDate%3C" + URLEncoder.encode(formattedCurrentDate,"UTF-8")
              + "&limit=11&offset=0")));
          testContext.completeNow();
        } catch (UnsupportedEncodingException e) {
          testContext.failNow(e);
        } finally {
          System.clearProperty(REPOSITORY_MAX_RECORDS_PER_RESPONSE);
        }
      })
    );
  }

  @Test
  @ExtendWith(VertxExtension.class)
  void buildItemsEndpointWithSuppressDiscoveryFalse(Vertx vertx, VertxTestContext testContext) {
    vertx.runOnContext(event ->
      testContext.verify(() ->  {
        try {
          System.setProperty(REPOSITORY_MAX_RECORDS_PER_RESPONSE, "10");
          System.setProperty(REPOSITORY_SUPPRESSED_RECORDS_PROCESSING, "true");
          Map<String, String> okapiHeaders = new HashMap<>();
          okapiHeaders.put(OKAPI_TENANT, EXIST_CONFIG_TENANT);
          String formattedCurrentDate = getFormattedCurrentDate();
          assertThat(getStorageHelper(SOURCE_RECORD_STORAGE).buildRecordsEndpoint(Request.builder().okapiHeaders(okapiHeaders).build(), false), is
            (equalTo(SOURCE_STORAGE_RESULT_URI + "?query=recordType%3D%3DMARC+and+metadata.updatedDate%3C"
              + URLEncoder.encode(formattedCurrentDate,"UTF-8") + "&limit=11&offset=0")));
          testContext.completeNow();
        } catch (UnsupportedEncodingException e) {
          testContext.failNow(e);
        } finally {
          System.clearProperty(REPOSITORY_MAX_RECORDS_PER_RESPONSE);
        }
      })
    );
  }

  @ParameterizedTest
  @ValueSource(strings = { SOURCE_RECORD_STORAGE, INVENTORY_STORAGE })
  void getId(String storageType) {
    JsonObject item = getJsonObjectFromFile(getDirPathWithPathRecordsInSRS(storageType) + "/instance.json");
    assertThat(getStorageHelper(storageType).getId(item), not(isEmptyOrNullString()));
  }

  @ParameterizedTest
  @ValueSource(strings = { SOURCE_RECORD_STORAGE, INVENTORY_STORAGE })
  void getRecordsItems(String storageType) {
    JsonObject entries = getJsonObjectFromFile(getDirPathWithPathRecordsInSRS(storageType) + "/marc-e567b8e2-a45b-45f1-a85a-6b6312bdf4d8.json");
    JsonArray items = getStorageHelper(storageType).getRecordsItems(entries);
    assertThat(items, is(notNullValue()));
    assertThat(items, is(iterableWithSize(1)));
  }

  @Test
  void getIdentifierIdFromInventtoryStorage() {
    JsonObject item = getJsonObjectFromFile(getDirPath("//TODO CHANGE")+"/instance.json");
    assertThat(getStorageHelper(INVENTORY_STORAGE).getIdentifierId(item), not(isEmptyOrNullString()));
  }


  private StorageHelper getStorageHelper(String storageType) {
    System.setProperty(REPOSITORY_STORAGE, storageType);
    return StorageHelper.getInstance();
  }

  private String getDirPath(String storageType) {
    return SOURCE_RECORD_STORAGE.equals(storageType) ? SOURCE_STORAGE_RESULT_URI : "TODO CHANGE";
  }

  private String getDirPathWithPathRecordsInSRS(String storageType) {
    return SOURCE_RECORD_STORAGE.equals(storageType) ? SOURCE_STORAGE_RECORD_PATH : "TODO CHANGE";
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

  private String getFormattedCurrentDate(){
    return dateTimeFormatter.format(LocalDate.parse(LocalDateTime.now(ZoneOffset.UTC)
      .format(ISO_UTC_DATE_ONLY))
      .atStartOfDay()
      .plusDays(1L));
  }
}
