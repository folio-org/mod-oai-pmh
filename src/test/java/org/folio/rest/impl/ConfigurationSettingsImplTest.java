package org.folio.rest.impl;

import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.http.Header;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.config.ApplicationConfig;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.common.TestUtil;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.service.ConfigurationSettingsService;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.ConfigValue;
import org.folio.rest.jaxrs.model.ConfigurationSettings;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.spring.SpringContextUtil;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;

@ExtendWith(VertxExtension.class)
@TestInstance(PER_CLASS)
class ConfigurationSettingsImplTest {

  @Autowired
  private ConfigurationSettingsImpl configurationSettingsImpl;

  @Autowired
  private ConfigurationSettingsService configurationSettingsService;

  private static final Logger logger = LogManager.getLogger(ConfigurationSettingsImplTest.class);

  private static final int okapiPort = NetworkUtils.nextFreePort();
  private static final int mockPort = NetworkUtils.nextFreePort();
  private static final String CONFIGURATION_SETTINGS_PATH = "/oai-pmh/configuration-settings";

  private Header tenantHeader;
  private Header okapiUrlHeader;
  private Header okapiUserHeader;

  private Vertx vertx;

  @BeforeAll
  void setUpOnce(Vertx vertx, VertxTestContext testContext) {
    this.vertx = vertx;
    tenantHeader = new Header("X-Okapi-Tenant", OAI_TEST_TENANT);
    okapiUrlHeader = new Header("X-Okapi-Url", "http://localhost:" + mockPort);
    okapiUserHeader = new Header("X-Okapi-User-Id", UUID.randomUUID().toString());

    PostgresClientFactory.setShouldResetPool(true);
    RestAssured.baseURI = "http://localhost:" + okapiPort;
    RestAssured.port = okapiPort;
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient client = PostgresClient.getInstance(vertx, OAI_TEST_TENANT);
    client.startPostgresTester();
    TestUtil.initializeTestContainerDbSchema(vertx, OAI_TEST_TENANT);

    JsonObject dpConfig = new JsonObject();
    dpConfig.put("http.port", okapiPort);
    DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(dpConfig);
    WebClientProvider.init(vertx);
    vertx.deployVerticle(
          RestVerticle.class.getName(),
          deploymentOptions,
          testContext.succeeding(v -> {
            try {
              Context context = vertx.getOrCreateContext();
              SpringContextUtil.init(vertx, context, ApplicationConfig.class);
              SpringContextUtil.autowireDependencies(this, context);

              OkapiMockServer mockServer = new OkapiMockServer(vertx, mockPort);
              mockServer.start(testContext);

              testContext.completeNow();
            } catch (Exception e) {
              testContext.failNow(e);
            }
          })
    );
  }

  @AfterAll
  void afterAll() {
    WebClientProvider.closeAll();
    PostgresClientFactory.closeAll();
  }

  @BeforeEach
  void cleanTable() {
    PostgresClient client = PostgresClient.getInstance(vertx, OAI_TEST_TENANT);
    client.execute("DELETE FROM " + OAI_TEST_TENANT + "_mod_oai_pmh.configuration_settings")
        .onComplete(ar -> {
          if (ar.failed()) {
            logger.warn("Failed to clean configuration_settings table: " + ar.cause().getMessage());
          }
        });
  }

  private RequestSpecification createBaseRequest(String path, ContentType contentType) {
    RequestSpecification request = RestAssured.given()
        .header(tenantHeader)
        .header(okapiUrlHeader)
        .header(okapiUserHeader);
    if (contentType != null) {
      request.contentType(contentType);
    }
    return request.basePath(path);
  }

  private String getConfigurationSettingsPathWithId(String id) {
    return CONFIGURATION_SETTINGS_PATH + "/" + id;
  }

  private JsonObject createTestConfig(String configName) {
    return new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("configName", configName)
        .put("configValue", new JsonObject()
            .put("deletedRecordsSupport", "no")
            .put("suppressedRecordsProcessing", false)
            .put("errorsProcessing", "500")
            .put("recordsSource", "Source record storage"));
  }

  @Test
  void shouldCreateConfigurationSetting(VertxTestContext testContext) {
    JsonObject config = createTestConfig("test-behavioral");
    String id = config.getString("id");

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode())
          .when()
          .post()
          .then()
          .statusCode(201)
          .body("id", equalTo(id))
          .body("configName", equalTo("test-behavioral"))
          .body("configValue.deletedRecordsSupport", equalTo("no"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldUpdateConfigurationSetting(VertxTestContext testContext) {
    JsonObject config = createTestConfig("test-update");
    String id = config.getString("id");

    JsonObject updatedConfig = new JsonObject()
        .put("configName", "test-update")
        .put("configValue", new JsonObject()
            .put("deletedRecordsSupport", "persistent")
            .put("suppressedRecordsProcessing", true)
            .put("errorsProcessing", "1000")
            .put("recordsSource", "Inventory"));

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode()).when().post().then().statusCode(201);

      createBaseRequest(getConfigurationSettingsPathWithId(id), ContentType.JSON)
          .body(updatedConfig.encode()).when().put().then()
          .statusCode(200)
          .body("id", equalTo(id))
          .body("configValue.deletedRecordsSupport", equalTo("persistent"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldDeleteConfigurationSetting(VertxTestContext testContext) {
    JsonObject config = createTestConfig("test-delete");
    String id = config.getString("id");

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode()).when().post().then().statusCode(201);

      createBaseRequest(getConfigurationSettingsPathWithId(id), null)
          .when().delete().then().statusCode(204);

      createBaseRequest(getConfigurationSettingsPathWithId(id), null)
          .when().get().then().statusCode(404);

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnConfigurationSettingsList(VertxTestContext testContext) {
    JsonObject config1 = createTestConfig("test-list-1");
    JsonObject config2 = createTestConfig("test-list-2");

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config1.encode()).when().post().then().statusCode(201);
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config2.encode()).when().post().then().statusCode(201);

      createBaseRequest(CONFIGURATION_SETTINGS_PATH, null)
          .queryParam("offset", 0)
          .queryParam("limit", 10)
          .when().get()
          .then()
          .statusCode(200)
          .body("totalRecords", equalTo(2))
          .body("configurationSettings.size()", equalTo(2));

      testContext.completeNow();
    });
  }

  @Test
  void shouldGetConfigurationSettingById(VertxTestContext testContext) {
    JsonObject config = createTestConfig("test-get-by-id");
    String id = config.getString("id");

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode()).when().post().then().statusCode(201);

      createBaseRequest(getConfigurationSettingsPathWithId(id), null)
          .when().get()
          .then()
          .statusCode(200)
          .body("id", equalTo(id))
          .body("configName", equalTo("test-get-by-id"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturn404WhenConfigurationSettingNotFound(VertxTestContext testContext) {
    String nonExistentId = UUID.randomUUID().toString();

    testContext.verify(() -> {
      createBaseRequest(getConfigurationSettingsPathWithId(nonExistentId), null)
          .when().get()
          .then()
          .statusCode(404);

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturn404WhenUpdatingNonExistentConfiguration(VertxTestContext testContext) {
    String nonExistentId = UUID.randomUUID().toString();
    JsonObject updateConfig = new JsonObject()
        .put("configName", "test-update")
        .put("configValue", new JsonObject().put("enableOaiService", true));

    testContext.verify(() -> {
      createBaseRequest(getConfigurationSettingsPathWithId(nonExistentId), ContentType.JSON)
          .body(updateConfig.encode())
          .when().put()
          .then()
          .statusCode(404);

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturn404WhenDeletingNonExistentConfiguration(VertxTestContext testContext) {
    String nonExistentId = UUID.randomUUID().toString();

    testContext.verify(() -> {
      createBaseRequest(getConfigurationSettingsPathWithId(nonExistentId), null)
          .when().delete()
          .then()
          .statusCode(404);

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturn400WhenCreatingDuplicateConfiguration(VertxTestContext testContext) {
    JsonObject config = createTestConfig("test-duplicate");

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode()).when().post().then().statusCode(201);

      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode())
          .when().post()
          .then()
          .statusCode(400);

      testContext.completeNow();
    });
  }

  @Test
  void shouldGetConfigurationSettingByName(VertxTestContext testContext) {
    JsonObject config = createTestConfig("test-get-by-name");

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode()).when().post().then().statusCode(201);

      createBaseRequest(CONFIGURATION_SETTINGS_PATH + "/name/test-get-by-name", null)
          .when().get()
          .then()
          .statusCode(200)
          .body("configName", equalTo("test-get-by-name"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturn404WhenGettingConfigurationByNonExistentName(VertxTestContext testContext) {
    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH + "/name/non-existent-name", null)
          .when().get()
          .then()
          .statusCode(404);

      testContext.completeNow();
    });
  }

  @Test
  void shouldCreateConfigurationWithoutExplicitId(VertxTestContext testContext) {
    JsonObject config = new JsonObject()
        .put("configName", "test-auto-id")
        .put("configValue", new JsonObject()
            .put("enableOaiService", true)
            .put("repositoryName", "Test Repository"));

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode())
          .when().post()
          .then()
          .statusCode(201)
          .body("configName", equalTo("test-auto-id"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldHandlePaginationWithOffset(VertxTestContext testContext) {
    testContext.verify(() -> {
      for (int i = 0; i < 5; i++) {
        JsonObject config = createTestConfig("test-pagination-" + i);
        createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
            .body(config.encode()).when().post().then().statusCode(201);
      }

      // Test pagination
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, null)
          .queryParam("offset", 2)
          .queryParam("limit", 2)
          .when().get()
          .then()
          .statusCode(200)
          .body("configurationSettings.size()", equalTo(2));

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnEmptyListWhenNoConfigurations(VertxTestContext testContext) {
    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, null)
          .queryParam("offset", 0)
          .queryParam("limit", 10)
          .when().get()
          .then()
          .statusCode(200)
          .body("totalRecords", equalTo(0))
          .body("configurationSettings.size()", equalTo(0));

      testContext.completeNow();
    });
  }

  @Test
  void shouldUpdateOnlyConfigValue(VertxTestContext testContext) {
    JsonObject config = createTestConfig("test-partial-update");
    String id = config.getString("id");

    JsonObject partialUpdate = new JsonObject()
        .put("configName", "test-partial-update")
        .put("configValue", new JsonObject()
            .put("maxRecordsPerResponse", 50)
            .put("enableLogging", false));

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode()).when().post().then().statusCode(201);

      createBaseRequest(getConfigurationSettingsPathWithId(id), ContentType.JSON)
          .body(partialUpdate.encode())
          .when().put()
          .then()
          .statusCode(200)
          .body("configValue.maxRecordsPerResponse", equalTo(50));

      testContext.completeNow();
    });
  }

  @Test
  void shouldHandleComplexConfigValue(VertxTestContext testContext) {
    JsonObject complexConfig = new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("configName", "test-complex")
        .put("configValue", new JsonObject()
            .put("nested", new JsonObject()
                .put("level1", new JsonObject()
                    .put("level2", "value")))
            .put("array", new JsonObject()
                .put("items", new String[]{"item1", "item2", "item3"}))
            .put("boolean", true)
            .put("number", 123));

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(complexConfig.encode())
          .when().post()
          .then()
          .statusCode(201)
          .body("configValue.boolean", equalTo(true))
          .body("configValue.number", equalTo(123));

      testContext.completeNow();
    });
  }

  @Test
  void shouldHandleSpecialCharactersInConfigName(VertxTestContext testContext) {
    JsonObject config = new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("configName", "test-config_with-special.chars")
        .put("configValue", new JsonObject().put("key", "value"));

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode())
          .when().post()
          .then()
          .statusCode(201)
          .body("configName", equalTo("test-config_with-special.chars"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnAllConfigurationsWithDefaultPagination(VertxTestContext testContext) {
    testContext.verify(() -> {
      for (int i = 0; i < 3; i++) {
        JsonObject config = createTestConfig("test-default-pagination-" + i);
        createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
            .body(config.encode()).when().post().then().statusCode(201);
      }

      createBaseRequest(CONFIGURATION_SETTINGS_PATH, null)
          .when().get()
          .then()
          .statusCode(200)
          .body("configurationSettings.size()", equalTo(3));

      testContext.completeNow();
    });
  }

  @Test
  void shouldHandleLargeConfigValue(VertxTestContext testContext) {
    StringBuilder largeValue = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      largeValue.append("data").append(i);
    }

    JsonObject config = new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("configName", "test-large-value")
        .put("configValue", new JsonObject().put("largeField", largeValue.toString()));

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode())
          .when().post()
          .then()
          .statusCode(201);

      testContext.completeNow();
    });
  }

  @Test
  void shouldUpdateConfigurationMultipleTimes(VertxTestContext testContext) {
    JsonObject config = createTestConfig("test-multiple-updates");
    String id = config.getString("id");

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode()).when().post().then().statusCode(201);

      JsonObject update1 = new JsonObject()
          .put("configName", "test-multiple-updates")
          .put("configValue", new JsonObject().put("version", "1"));
      createBaseRequest(getConfigurationSettingsPathWithId(id), ContentType.JSON)
          .body(update1.encode()).when().put().then().statusCode(200);

      JsonObject update2 = new JsonObject()
          .put("configName", "test-multiple-updates")
          .put("configValue", new JsonObject().put("version", "2"));
      createBaseRequest(getConfigurationSettingsPathWithId(id), ContentType.JSON)
          .body(update2.encode()).when().put().then().statusCode(200)
          .body("configValue.version", equalTo("2"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldHandleEmptyConfigValue(VertxTestContext testContext) {
    JsonObject config = new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("configName", "test-empty-value")
        .put("configValue", new JsonObject());

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode())
          .when().post()
          .then()
          .statusCode(201)
          .body("configName", equalTo("test-empty-value"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldGetConfigurationAndVerifyAllFields(VertxTestContext testContext) {
    JsonObject config = new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("configName", "test-verify-fields")
        .put("configValue", new JsonObject()
            .put("field1", "value1")
            .put("field2", true)
            .put("field3", 42));

    String id = config.getString("id");

    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(config.encode()).when().post().then().statusCode(201);

      createBaseRequest(getConfigurationSettingsPathWithId(id), null)
          .when().get()
          .then()
          .statusCode(200)
          .body("id", equalTo(id))
          .body("configName", equalTo("test-verify-fields"))
          .body("configValue.field1", equalTo("value1"))
          .body("configValue.field2", equalTo(true))
          .body("configValue.field3", equalTo(42));

      testContext.completeNow();
    });
  }

  @Test
  void shouldSaveAndRetrieveConfigurationSettingThroughApi1(VertxTestContext testContext) {
    String id = UUID.randomUUID().toString();
    ConfigurationSettings config = new ConfigurationSettings()
          .withId(id)
          .withConfigName("test-dao")
          .withConfigValue(new ConfigValue()
              .withAdditionalProperty("enableOaiService", true)
              .withAdditionalProperty("repositoryName", "Test Repository"));

    JsonObject jsonConfig = new JsonObject()
          .put("id", config.getId())
          .put("configName", config.getConfigName())
          .put("configValue", new JsonObject()
          .put("enableOaiService", true)
          .put("repositoryName", "Test Repository"));

    configurationSettingsService.saveConfigurationSettings(jsonConfig, OAI_TEST_TENANT, "test-user")
          .compose(saved -> configurationSettingsService
        .getConfigurationSettingsById(id, OAI_TEST_TENANT))
          .onComplete(ar -> {
            if (ar.failed()) {
              testContext.failNow(ar.cause());
              return;
            }

            JsonObject serviceResult = ar.result();
            try {
              io.restassured.response.Response apiResponse = createBaseRequest(
                    getConfigurationSettingsPathWithId(id), null)
                      .when().get();
              testContext.verify(() -> {
                assertEquals(200, apiResponse.getStatusCode());
                JsonObject apiJson = new JsonObject(apiResponse.getBody().asString());
                assertEquals(serviceResult.encode(), apiJson.encode());
                testContext.completeNow();
              });
            } catch (Exception e) {
              testContext.failNow(e);
            }
          });
  }
}
