package org.folio.rest.impl;

import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.hamcrest.Matchers.equalTo;
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
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.spring.SpringContextUtil;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
@TestInstance(PER_CLASS)
class ConfigurationSettingsImplTest {

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
}
