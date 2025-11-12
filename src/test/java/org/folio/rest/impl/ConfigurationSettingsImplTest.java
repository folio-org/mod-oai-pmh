package org.folio.rest.impl;

import static io.restassured.RestAssured.given;
import static org.folio.oaipmh.Constants.REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC;
import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.http.Header;
import io.restassured.parsing.Parser;
import io.restassured.response.Response;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.http.HttpStatus;
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


@NotThreadSafe
@TestInstance(PER_CLASS)
@ExtendWith(VertxExtension.class)
class ConfigurationSettingsImplTest {

  private static final Logger logger = LogManager.getLogger(ConfigurationSettingsImplTest.class);

  private static final String CONFIGURATION_SETTINGS_PATH = "/oai-pmh/configuration-settings";
  private static final String TENANT_ID = OAI_TEST_TENANT;
  private static final String USER_ID = "test-user-id";

  private static final int okapiPort = NetworkUtils.nextFreePort();
  private static final int mockPort = NetworkUtils.nextFreePort();

  private final Header tenantHeader = new Header("X-Okapi-Tenant", TENANT_ID);
  private final Header userIdHeader = new Header("X-Okapi-User-Id", USER_ID);
  private final Header urlHeader = new Header("X-Okapi-Url", "http://localhost:9130");
  private final Header tokenHeader = new Header("X-Okapi-Token", "test-token");

  private static final int serverPort = NetworkUtils.nextFreePort();
  private PostgresTesterContainer postgresTesterContainer;
  private String idleTimeout;
  private static Vertx vertx;

  @BeforeAll
  void setUpClass(VertxTestContext testContext) throws Exception {
    vertx = Vertx.vertx();

    PostgresClient.setPostgresTester(postgresTesterContainer = new PostgresTesterContainer());
    PostgresClient.getInstance(vertx, OAI_TEST_TENANT).startPostgresTester();

    TestUtil.initializeTestContainerDbSchema(vertx, TENANT_ID);

    RestAssured.port = serverPort;
    RestAssured.baseURI = "http://localhost";
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

    DeploymentOptions options = new DeploymentOptions()
        .setConfig(new JsonObject()
        .put("http.port", serverPort));

    WebClientProvider.init(vertx);
    vertx.deployVerticle(RestVerticle.class.getName(), options, testContext.succeeding(id -> {
      idleTimeout = System.getProperty(REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC);
      System.setProperty(REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC, "1");
      Context context = vertx.getOrCreateContext();
      SpringContextUtil.init(vertx, context, ApplicationConfig.class);
      SpringContextUtil.autowireDependencies(this, context);
      logger.info("mod-oai-pmh Test: setup done. Using port " + okapiPort);
      new OkapiMockServer(vertx, mockPort).start(testContext);
    }));
    assertTrue(testContext.awaitCompletion(10, TimeUnit.SECONDS));
  }

  @AfterAll
  void tearDownClass(VertxTestContext testContext) {
    PostgresClientFactory.closeAll();
    vertx.close(testContext.succeeding(res -> testContext.completeNow()));
  }

  @Test
  void shouldCreateConfigurationSettings() {
    JsonObject configValue = new JsonObject()
        .put("deletedRecordsSupport", "no")
        .put("suppressedRecordsProcessing", false)
        .put("errorsProcessing", "500")
        .put("recordsSource", "Source record storage");

    JsonObject config = new JsonObject()
        .put("configName", "test-behavioral-" + UUID.randomUUID())
        .put("configValue", configValue);

    Response response = given()
        .header(tenantHeader)
        .header(userIdHeader)
        .header(urlHeader)
        .header(tokenHeader)
        .contentType(ContentType.JSON)
        .body(config.encode())
        .when()
        .post(CONFIGURATION_SETTINGS_PATH)
        .then()
        .defaultParser(Parser.JSON)
        .statusCode(HttpStatus.SC_CREATED)
        .body("id", notNullValue())
        .body("configName", notNullValue())
        .body("configValue.deletedRecordsSupport", equalTo("no"))
        .body("configValue.suppressedRecordsProcessing", equalTo(false))
        .extract().response();

    String createdId = response.jsonPath().getString("id");
    assertNotNull(createdId);
  }

  @Test
  void shouldGetConfigurationSettingsList() {
    JsonObject configValue1 = new JsonObject()
        .put("deletedRecordsSupport", "persistent")
        .put("suppressedRecordsProcessing", true)
        .put("errorsProcessing", "1000")
        .put("recordsSource", "Inventory");
    JsonObject configValue2 = new JsonObject()
        .put("deletedRecordsSupport", "persistent")
        .put("suppressedRecordsProcessing", true)
        .put("errorsProcessing", "1000")
        .put("recordsSource", "Inventory");

    JsonObject config = new JsonObject()
        .put("configName", "test-behavioral-1" + UUID.randomUUID())
        .put("configValue", configValue1)
        .put("configName", "test-behavioral-2" + UUID.randomUUID())
        .put("configValue", configValue2);

    given()
        .header(tenantHeader)
        .header(urlHeader)
        .header(tokenHeader)
        .contentType(ContentType.JSON)
        .body(config.encode())
        .queryParam("offset", 0)
        .queryParam("limit", 2)
        .when()
        .get(CONFIGURATION_SETTINGS_PATH)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .defaultParser(Parser.JSON)
          .body("totalRecords", greaterThanOrEqualTo(2));
  }

  @Test
  void shouldGetConfigurationSettingsById() {
    String createdId = createTestConfig("get-by-id-test");

    given()
      .header(tenantHeader)
      .header(urlHeader)
      .header(tokenHeader)
      .contentType(ContentType.JSON)
      .when()
      .get(CONFIGURATION_SETTINGS_PATH + "/" + createdId)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .defaultParser(Parser.JSON)
      .body("id", equalTo(createdId))
      .body("configName", equalTo("get-by-id-test"))
        .body("configValue", notNullValue());
  }

  @Test
  void shouldReturn404WhenConfigNotFound() {
    String nonExistentId = UUID.randomUUID().toString();

    given()
      .header(tenantHeader)
      .header(urlHeader)
      .header(tokenHeader)
      .when()
      .get(CONFIGURATION_SETTINGS_PATH + "/" + nonExistentId)
      .then()
        .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  void shouldUpdateConfigurationSettings() {
    String createdId = createTestConfig("update-test");

    JsonObject updatedConfigValue = new JsonObject()
        .put("deletedRecordsSupport", "persistent")
        .put("suppressedRecordsProcessing", true)
        .put("errorsProcessing", "1000")
        .put("recordsSource", "Inventory");

    JsonObject updatedConfig = new JsonObject()
        .put("configName", "update-test")
        .put("configValue", updatedConfigValue);

    given()
      .header(tenantHeader)
      .header(userIdHeader)
      .header(urlHeader)
      .header(tokenHeader)
      .contentType(ContentType.JSON)
      .body(updatedConfig.encode())
      .when()
      .put(CONFIGURATION_SETTINGS_PATH + "/" + createdId)
      .then()
      .defaultParser(Parser.JSON)
      .statusCode(HttpStatus.SC_OK)
      .body("id", equalTo(createdId))
      .body("configValue.deletedRecordsSupport", equalTo("persistent"))
      .body("configValue.suppressedRecordsProcessing", equalTo(true))
        .body("configValue.errorsProcessing", equalTo("1000"));
  }

  @Test
  void shouldReturn404WhenUpdatingNonExistentConfig() {
    String nonExistentId = UUID.randomUUID().toString();

    JsonObject config = new JsonObject()
        .put("configName", "test")
        .put("configValue", new JsonObject().put("key", "value"));

    given()
      .header(tenantHeader)
      .header(userIdHeader)
      .header(urlHeader)
      .header(tokenHeader)
      .contentType(ContentType.JSON)
      .body(config.encode())
      .when()
      .put(CONFIGURATION_SETTINGS_PATH + "/" + nonExistentId)
      .then()
        .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  void shouldDeleteConfigurationSettings() {
    String createdId = createTestConfig("delete-test");

    given()
      .header(tenantHeader)
      .header(urlHeader)
      .header(tokenHeader)
      .when()
      .delete(CONFIGURATION_SETTINGS_PATH + "/" + createdId)
      .then()
        .statusCode(HttpStatus.SC_NO_CONTENT);

    given()
      .header(tenantHeader)
      .header(urlHeader)
      .header(tokenHeader)
      .when()
      .get(CONFIGURATION_SETTINGS_PATH + "/" + createdId)
      .then()
        .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  void shouldReturn404WhenDeletingNonExistentConfig() {
    String nonExistentId = UUID.randomUUID().toString();

    given()
      .header(tenantHeader)
      .header(urlHeader)
      .header(tokenHeader)
      .when()
      .delete(CONFIGURATION_SETTINGS_PATH + "/" + nonExistentId)
      .then()
        .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  void shouldValidateRequiredFieldsWhenCreating() {
    JsonObject invalidConfig = new JsonObject()
        .put("configValue", new JsonObject().put("key", "value"));

    given()
      .header(tenantHeader)
      .header(userIdHeader)
      .header(urlHeader)
      .header(tokenHeader)
      .contentType(ContentType.JSON)
      .body(invalidConfig.encode())
      .when()
      .post(CONFIGURATION_SETTINGS_PATH)
      .then()
        .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  void shouldHandlePaginationWithOffsetAndLimit() {
    JsonObject configValue1 = new JsonObject()
        .put("deletedRecordsSupport", "persistent")
        .put("suppressedRecordsProcessing", true)
        .put("errorsProcessing", "1000")
        .put("recordsSource", "Inventory");
    JsonObject configValue2 = new JsonObject()
        .put("deletedRecordsSupport", "persistent")
        .put("suppressedRecordsProcessing", true)
        .put("errorsProcessing", "1000")
        .put("recordsSource", "Inventory");

    JsonObject config = new JsonObject()
        .put("configName", "test-behavioral-1" + UUID.randomUUID())
        .put("configValue", configValue1)
        .put("configName", "test-behavioral-2" + UUID.randomUUID())
        .put("configValue", configValue2);

    given()
        .header(tenantHeader)
        .header(urlHeader)
        .header(tokenHeader)
        .contentType(ContentType.JSON)
        .body(config.encode())
        .queryParam("offset", 0)
        .queryParam("limit", 2)
        .when()
        .get(CONFIGURATION_SETTINGS_PATH)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .defaultParser(Parser.JSON)
        .body("totalRecords", greaterThanOrEqualTo(2))
        .extract().response();
  }

  @Test
  void shouldReturn400WithInvalidJsonPayload() {
    given()
      .header(tenantHeader)
      .header(userIdHeader)
      .header(urlHeader)
      .header(tokenHeader)
      .contentType(ContentType.JSON)
      .body("{ invalid json }")
      .when()
      .post(CONFIGURATION_SETTINGS_PATH)
      .then()
        .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  void shouldCreateMultipleConfigurationsWithDifferentNames() {
    String config1Id = createTestConfig("config-1-" + UUID.randomUUID());
    String config2Id = createTestConfig("config-2-" + UUID.randomUUID());
    String config3Id = createTestConfig("config-3-" + UUID.randomUUID());

    assertNotNull(config1Id);
    assertNotNull(config2Id);
    assertNotNull(config3Id);

    given()
      .header(tenantHeader)
      .header(urlHeader)
      .header(tokenHeader)
      .when()
      .get(CONFIGURATION_SETTINGS_PATH + "/" + config1Id)
      .then()
        .statusCode(HttpStatus.SC_OK);

    given()
      .header(tenantHeader)
      .header(urlHeader)
      .header(tokenHeader)
      .when()
      .get(CONFIGURATION_SETTINGS_PATH + "/" + config2Id)
      .then()
        .statusCode(HttpStatus.SC_OK);

    given()
      .header(tenantHeader)
      .header(urlHeader)
      .header(tokenHeader)
      .when()
      .get(CONFIGURATION_SETTINGS_PATH + "/" + config3Id)
      .then()
        .statusCode(HttpStatus.SC_OK);
  }

  @Test
  void shouldFilterConfigurationSettingsByName() {
    // Create test configurations with unique names
    String configName1 = "test-filter-config-" + UUID.randomUUID();
    String configName2 = "test-filter-config-" + UUID.randomUUID();
    String configName3 = "another-config-" + UUID.randomUUID();
    
    String config1Id = createTestConfig(configName1);
    createTestConfig(configName2);
    createTestConfig(configName3);

    // Filter by first config name - should return only 1 result
    given()
        .header(tenantHeader)
        .header(urlHeader)
        .header(tokenHeader)
        .queryParam("name", configName1)
        .when()
        .get(CONFIGURATION_SETTINGS_PATH)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .defaultParser(Parser.JSON)
        .body("totalRecords", equalTo(1))
        .body("configurationSettings.size()", equalTo(1))
        .body("configurationSettings[0].configName", equalTo(configName1))
        .body("configurationSettings[0].id", equalTo(config1Id));
  }

  @Test
  void shouldReturnEmptyListWhenNameFilterMatchesNoResults() {
    String nonExistentName = "non-existent-config-" + UUID.randomUUID();

    given()
        .header(tenantHeader)
        .header(urlHeader)
        .header(tokenHeader)
        .queryParam("name", nonExistentName)
        .when()
        .get(CONFIGURATION_SETTINGS_PATH)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .defaultParser(Parser.JSON)
        .body("totalRecords", equalTo(0))
        .body("configurationSettings.size()", equalTo(0));
  }

  @Test
  void shouldReturnAllConfigurationsWhenNoNameFilterProvided() {
    // Create multiple test configurations
    String configName1 = "test-all-configs-1-" + UUID.randomUUID();
    String configName2 = "test-all-configs-2-" + UUID.randomUUID();
    
    createTestConfig(configName1);
    createTestConfig(configName2);

    // Get all configurations without name filter
    given()
        .header(tenantHeader)
        .header(urlHeader)
        .header(tokenHeader)
        .queryParam("limit", 100)
        .when()
        .get(CONFIGURATION_SETTINGS_PATH)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .defaultParser(Parser.JSON)
        .body("totalRecords", greaterThanOrEqualTo(2));
  }

  @Test
  void shouldHandleNameFilterWithPagination() {
    String configName = "test-pagination-filter-" + UUID.randomUUID();
    createTestConfig(configName);

    // Test with name filter and pagination parameters
    given()
        .header(tenantHeader)
        .header(urlHeader)
        .header(tokenHeader)
        .queryParam("name", configName)
        .queryParam("offset", 0)
        .queryParam("limit", 10)
        .when()
        .get(CONFIGURATION_SETTINGS_PATH)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .defaultParser(Parser.JSON)
        .body("totalRecords", equalTo(1))
        .body("configurationSettings.size()", equalTo(1))
        .body("configurationSettings[0].configName", equalTo(configName));
  }

  /**.
   * Helper method to create a test configuration

   * @param configName the name of the configuration
   * @return the ID of the created configuration
   */
  private String createTestConfig(String configName) {
    JsonObject configValue = new JsonObject()
        .put("deletedRecordsSupport", "no")
        .put("suppressedRecordsProcessing", false)
        .put("errorsProcessing", "500")
        .put("recordsSource", "Source record storage");

    JsonObject config = new JsonObject()
        .put("configName", configName)
        .put("configValue", configValue);

    Response response = given()
        .header(tenantHeader)
        .header(userIdHeader)
        .header(urlHeader)
        .header(tokenHeader)
        .contentType(ContentType.JSON)
        .body(config.encode())
        .when()
        .post(CONFIGURATION_SETTINGS_PATH)
        .then()
        .defaultParser(Parser.JSON)
        .statusCode(HttpStatus.SC_CREATED)
        .extract().response();

    return response.jsonPath().getString("id");
  }
}
