package org.folio.rest.impl;

import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.config.ApplicationConfig;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.common.TestUtil;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.spring.SpringContextUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
@TestInstance(PER_CLASS)
class ConfigurationSettingsImplTest {

  private static final Logger logger = LogManager.getLogger(ConfigurationSettingsImplTest.class);

  private static final int okapiPort = NetworkUtils.nextFreePort();
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final String CONFIGURATION_SETTINGS_PATH = "/oai-pmh/configuration-settings";

  private Header tenantHeader = new Header("X-Okapi-Tenant", OAI_TEST_TENANT);
  private Header okapiUrlHeader = new Header("X-Okapi-Url", "http://localhost:" + mockPort);
  private Header okapiUserHeader = new Header("X-Okapi-User-Id", OkapiMockServer.TEST_USER_ID);

  // Test data
  private static final String TEST_CONFIG_ID = "123e4567-e89b-12d3-a456-426614174000";
  private static final String NONEXISTENT_CONFIG_ID = "123e4567-e89b-12d3-a456-426614174999";

  private static final JsonObject TEST_BEHAVIORAL_CONFIG = new JsonObject()
      .put("id", TEST_CONFIG_ID)
      .put("configName", "behavioral")
      .put("configValue", new JsonObject()
      .put("deletedRecordsSupport", "no")
      .put("suppressedRecordsProcessing", false)
      .put("errorsProcessing", "500")
      .put("recordsSource", "Source record storage"));

  private static final JsonObject TEST_GENERAL_CONFIG = new JsonObject()
      .put("configName", "general")
      .put("configValue", new JsonObject()
      .put("enableOaiService", true)
      .put("repositoryName", "FOLIO Repository"));

  private static final JsonObject UPDATED_CONFIG = new JsonObject()
      .put("configName", "behavioral")
      .put("configValue", new JsonObject()
      .put("deletedRecordsSupport", "persistent")
      .put("suppressedRecordsProcessing", true)
      .put("errorsProcessing", "1000")
      .put("recordsSource", "Inventory"));

  @BeforeAll
  void setUpOnce(Vertx vertx, VertxTestContext testContext) throws Exception {
    logger.info("Test setup starting for Configuration Settings tests.");
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
    vertx.deployVerticle(RestVerticle.class.getName(), deploymentOptions,
        testContext.succeeding(v -> {
          try {
            Context context = vertx.getOrCreateContext();
            SpringContextUtil.init(vertx, context, ApplicationConfig.class);
            SpringContextUtil.autowireDependencies(this, context);
            new OkapiMockServer(vertx, mockPort).start(testContext);
            testContext.completeNow();
          } catch (Exception e) {
            testContext.failNow(e);
          }
        }));
  }

  @AfterAll
  void afterAll() {
    WebClientProvider.closeAll();
    PostgresClientFactory.closeAll();
  }

  private RequestSpecification createBaseRequest(String path, ContentType contentType) {
    RequestSpecification request = RestAssured.given()
        .header(tenantHeader)
        .header(okapiUrlHeader)
        .header(okapiUserHeader);
    if (contentType != null) {
      request = request.contentType(contentType);
    }
    return request.basePath(path);
  }

  private String getConfigurationSettingsPathWithId(String id) {
    return CONFIGURATION_SETTINGS_PATH + "/" + id;
  }

  @Test
  void shouldCreateConfigurationSettingWhenValidDataProvided(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(CONFIGURATION_SETTINGS_PATH,
            ContentType.JSON)
          .body(TEST_BEHAVIORAL_CONFIG.encode());

      request.when()
          .post()
          .then()
          .statusCode(201)
          .contentType(ContentType.JSON)
          .body("id", equalTo(TEST_CONFIG_ID))
          .body("configName", equalTo("behavioral"))
          .body("configValue.deletedRecordsSupport", equalTo("no"))
          .body("configValue.suppressedRecordsProcessing", equalTo(false))
          .body("configValue.errorsProcessing", equalTo("500"))
          .body("configValue.recordsSource", equalTo("Source record storage"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldCreateConfigurationSettingWithGeneratedIdWhenNoIdProvided(VertxTestContext
                                                                         testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(CONFIGURATION_SETTINGS_PATH,
            ContentType.JSON)
          .body(TEST_GENERAL_CONFIG.encode());

      request.when()
          .post()
          .then()
          .statusCode(201)
          .contentType(ContentType.JSON)
          .body("id", notNullValue())
          .body("configName", equalTo("general"))
          .body("configValue.enableOaiService", equalTo(true))
          .body("configValue.repositoryName", equalTo("FOLIO Repository"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnBadRequestWhenCreatingDuplicateConfiguration(VertxTestContext testContext) {
    testContext.verify(() -> {
      // First, create the configuration
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(TEST_BEHAVIORAL_CONFIG.encode())
          .when()
          .post()
          .then()
          .statusCode(201);

      // Try to create the same configuration again
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(TEST_BEHAVIORAL_CONFIG.encode())
          .when()
          .post()
          .then()
          .statusCode(400)
          .body(equalTo("Configuration setting with id '" + TEST_CONFIG_ID + "' already exists"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnConfigurationSettingWhenGetByIdAndExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      // First, create the configuration
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(TEST_BEHAVIORAL_CONFIG.encode())
          .when()
          .post()
          .then()
          .statusCode(201);

      // Then retrieve it
      createBaseRequest(getConfigurationSettingsPathWithId(TEST_CONFIG_ID), null)
          .when()
          .get()
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .body("id", equalTo(TEST_CONFIG_ID))
          .body("configName", equalTo("behavioral"))
          .body("configValue.deletedRecordsSupport", equalTo("no"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnNotFoundWhenGetByIdAndDoesNotExist(VertxTestContext testContext) {
    testContext.verify(() -> {
      createBaseRequest(getConfigurationSettingsPathWithId(NONEXISTENT_CONFIG_ID),
            null)
          .when()
          .get()
          .then()
          .statusCode(404)
          .body(equalTo("ConfigurationSettings with id '"
            + NONEXISTENT_CONFIG_ID + "' was not found"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldUpdateConfigurationSettingWhenValidDataProvided(VertxTestContext testContext) {
    testContext.verify(() -> {
      // First, create the configuration
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(TEST_BEHAVIORAL_CONFIG.encode())
          .when()
          .post()
          .then()
          .statusCode(201);

      // Then update it
      createBaseRequest(getConfigurationSettingsPathWithId(TEST_CONFIG_ID), ContentType.JSON)
          .body(UPDATED_CONFIG.encode())
          .when()
          .put()
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .body("id", equalTo(TEST_CONFIG_ID))
          .body("configName", equalTo("behavioral"))
          .body("configValue.deletedRecordsSupport", equalTo("persistent"))
          .body("configValue.suppressedRecordsProcessing", equalTo(true))
          .body("configValue.errorsProcessing", equalTo("1000"))
          .body("configValue.recordsSource", equalTo("Inventory"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnNotFoundWhenUpdatingNonExistentConfiguration(VertxTestContext testContext) {
    testContext.verify(() -> {
      createBaseRequest(getConfigurationSettingsPathWithId(NONEXISTENT_CONFIG_ID),
            ContentType.JSON)
          .body(UPDATED_CONFIG.encode())
          .when()
          .put()
          .then()
          .statusCode(404)
          .body(equalTo("ConfigurationSettings with id '"
            + NONEXISTENT_CONFIG_ID + "' was not found"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldDeleteConfigurationSettingWhenExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      // First, create the configuration
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(TEST_BEHAVIORAL_CONFIG.encode())
          .when()
          .post()
          .then()
          .statusCode(201);

      // Then delete it
      createBaseRequest(getConfigurationSettingsPathWithId(TEST_CONFIG_ID), null)
          .when()
          .delete()
          .then()
          .statusCode(204);

      // Verify it's gone
      createBaseRequest(getConfigurationSettingsPathWithId(TEST_CONFIG_ID), null)
          .when()
          .get()
          .then()
          .statusCode(404);

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnNotFoundWhenDeletingNonExistentConfiguration(VertxTestContext testContext) {
    testContext.verify(() -> {
      createBaseRequest(getConfigurationSettingsPathWithId(NONEXISTENT_CONFIG_ID),
            null)
          .when()
          .delete()
          .then()
          .statusCode(404)
          .body(equalTo("ConfigurationSettings with id '"
            + NONEXISTENT_CONFIG_ID + "' was not found"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnConfigurationSettingsByName(VertxTestContext testContext) {
    testContext.verify(() -> {
      // First, create the configuration
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(TEST_BEHAVIORAL_CONFIG.encode())
          .when()
          .post()
          .then()
          .statusCode(201);

      // Then retrieve by name
      createBaseRequest(CONFIGURATION_SETTINGS_PATH + "/name/behavioral", null)
          .when()
          .get()
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .body("id", equalTo(TEST_CONFIG_ID))
          .body("configName", equalTo("behavioral"))
          .body("configValue.deletedRecordsSupport", equalTo("no"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnNotFoundWhenGetByNameAndDoesNotExist(VertxTestContext testContext) {
    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH + "/name/nonexistent", null)
          .when()
          .get()
          .then()
          .statusCode(404)
          .body(equalTo("Configuration setting with name 'nonexistent' was not found"));

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnConfigurationSettingsList(VertxTestContext testContext) {
    testContext.verify(() -> {
      // Create multiple configurations
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(TEST_BEHAVIORAL_CONFIG.encode())
          .when()
          .post()
          .then()
          .statusCode(201);

      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(TEST_GENERAL_CONFIG.encode())
          .when()
          .post()
          .then()
          .statusCode(201);

      // Get the list
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, null)
          .queryParam("offset", 0)
          .queryParam("limit", 10)
          .when()
          .get()
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .body("totalRecords", equalTo(2))
          .body("configurationSettings", notNullValue())
          .body("configurationSettings.size()", equalTo(2));

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnPaginatedConfigurationSettingsList(VertxTestContext testContext) {
    testContext.verify(() -> {
      // Create multiple configurations
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(TEST_BEHAVIORAL_CONFIG.encode())
          .when()
          .post()
          .then()
          .statusCode(201);

      createBaseRequest(CONFIGURATION_SETTINGS_PATH, ContentType.JSON)
          .body(TEST_GENERAL_CONFIG.encode())
          .when()
          .post()
          .then()
          .statusCode(201);

      // Get the first page with limit 1
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, null)
          .queryParam("offset", 0)
          .queryParam("limit", 1)
          .when()
          .get()
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .body("totalRecords", equalTo(2))
          .body("configurationSettings", notNullValue())
          .body("configurationSettings.size()", equalTo(1));

      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnEmptyListWhenNoConfigurationSettings(VertxTestContext testContext) {
    testContext.verify(() -> {
      createBaseRequest(CONFIGURATION_SETTINGS_PATH, null)
          .queryParam("offset", 0)
          .queryParam("limit", 10)
          .when()
          .get()
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .body("totalRecords", equalTo(0))
          .body("configurationSettings", notNullValue())
          .body("configurationSettings.size()", equalTo(0));

      testContext.completeNow();
    });
  }
}
