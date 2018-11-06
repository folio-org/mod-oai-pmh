package org.folio.oaipmh.helpers;


import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.folio.oaipmh.Request;
import org.folio.rest.RestVerticle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;

import static org.folio.oaipmh.Constants.REPOSITORY_REQUEST_ITEMS_LIMIT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableWithSize.iterableWithSize;
import static org.hamcrest.text.IsEmptyString.isEmptyOrNullString;
import static org.junit.jupiter.api.Assertions.fail;


class InventoryStorageHelperTest {
  private static final Logger logger = LoggerFactory.getLogger(InventoryStorageHelperTest.class);
  private InventoryStorageHelper helper;


  @BeforeEach
  void init() {
    helper = new InventoryStorageHelper();
  }

  @Test
  void getItems() {
    JsonObject entries = getJsonObjectFromFile("/instance-storage/instances/instances_10.json");
    JsonArray items = helper.getItems(entries);
    assertThat(items, is(notNullValue()));
    assertThat(items, is(iterableWithSize(10)));
  }

  @Test
  void lastModifiedDate() {
    JsonObject item = getJsonObjectFromFile("/instance-storage/instances/instance.json");
    assertThat(helper.getLastModifiedDate(item), is(notNullValue()));
  }

  @Test
  void createdDate() {
    JsonObject item = getJsonObjectFromFile("/instance-storage/instances/instance_withCreatedDateOnly.json");
    assertThat(helper.getLastModifiedDate(item), is(notNullValue()));
  }

  @Test
  void epochDate() {
    JsonObject item = getJsonObjectFromFile("/instance-storage/instances/instance_withoutMetadata.json");
    assertThat(helper.getLastModifiedDate(item), is(Instant.EPOCH));
  }

  @Test
  void getItemId() {
    JsonObject item = getJsonObjectFromFile("/instance-storage/instances/instance.json");
    assertThat(helper.getItemId(item), not(isEmptyOrNullString()));
  }

  @Test
  @ExtendWith(VertxExtension.class)
  void buildItemsEndpoint(Vertx vertx, VertxTestContext testContext) {
    vertx.deployVerticle(RestVerticle.class.getName(), testContext.succeeding(s -> {
      testContext.verify(() ->  {
        try {
          Vertx.currentContext().config().put(REPOSITORY_REQUEST_ITEMS_LIMIT, "100");
          assertThat(helper.buildItemsEndpoint(Request.builder().build()), is
            (equalTo("/instance-storage/instances?query=sourceRecordFormat%3D%3DMARC-JSON&limit=100")));
          testContext.completeNow();
        } catch (UnsupportedEncodingException e) {
          testContext.failNow(e);
        }});

    }));

}

  /**
   * Creates {@link JsonObject} from the json file
   * @param path path to json file to read
   * @return {@link JsonObject} from the json file
   */
  private JsonObject getJsonObjectFromFile(String path) {
    try {
      File file = new File(InventoryStorageHelperTest.class.getResource(path).getFile());
      byte[] encoded = Files.readAllBytes(Paths.get(file.getPath()));
      return new JsonObject(new String(encoded, StandardCharsets.UTF_8));
    } catch (IOException e) {
      logger.error("Unexpected error", e);
      fail(e.getMessage());
    }
    return null;
  }
}
