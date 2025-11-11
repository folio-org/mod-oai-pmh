package org.folio.oaipmh.dao.impl;

import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.UUID;
import org.folio.oaipmh.common.TestUtil;
import org.folio.oaipmh.dao.ConfigurationSettingsDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.persist.PostgresClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the name search functionality in ConfigurationSettingsDao
 */
@TestInstance(PER_CLASS)
@ExtendWith(VertxExtension.class)
class ConfigurationSettingsDaoNameSearchTest {

  private static final String TEST_USER_ID = "test-user-id";
  private static PostgresClientFactory postgresClientFactory;
  private ConfigurationSettingsDao configurationSettingsDao;

  @BeforeAll
  void setUpClass(Vertx vertx, VertxTestContext testContext) {
    PostgresClientFactory.setShouldResetPool(true);
    postgresClientFactory = new PostgresClientFactory(vertx);
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient.getInstance(vertx, OAI_TEST_TENANT).startPostgresTester();

    configurationSettingsDao = new ConfigurationSettingsDaoImpl(postgresClientFactory);

    TestUtil.initializeTestContainerDbSchema(vertx, OAI_TEST_TENANT);
    testContext.completeNow();
  }

  @AfterAll
  void tearDownClass(Vertx vertx, VertxTestContext testContext) {
    PostgresClientFactory.closeAll();
    vertx.close(testContext.succeeding(res -> testContext.completeNow()));
  }

  @Test
  void shouldFilterConfigurationSettingsByName(VertxTestContext testContext) {
    String uniquePrefix = UUID.randomUUID().toString().substring(0, 8);
    Future<JsonObject> future1 = configurationSettingsDao.saveConfigurationSettings(
          createConfigWithName("behavior.deletedRecords." + uniquePrefix), OAI_TEST_TENANT, TEST_USER_ID);
    Future<JsonObject> future2 = configurationSettingsDao.saveConfigurationSettings(
          createConfigWithName("behavior.suppressedRecords." + uniquePrefix), OAI_TEST_TENANT, TEST_USER_ID);
    Future<JsonObject> future3 = configurationSettingsDao.saveConfigurationSettings(
          createConfigWithName("repository.name." + uniquePrefix), OAI_TEST_TENANT, TEST_USER_ID);

    Future.all(future1, future2, future3)
          .compose(v -> configurationSettingsDao
            .getConfigurationSettingsList(0, 10, "deletedRecords", OAI_TEST_TENANT))
          .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
            assertNotNull(result);
            assertTrue(result.getInteger("totalRecords") >= 1);
            JsonArray configs = result.getJsonArray("configurationSettings");
            assertNotNull(configs);
            // Verify that all returned configs contain "deletedRecords" in their name
            configs.stream()
                  .map(obj -> (JsonObject) obj)
                  .forEach(config -> {
                    String configName = config.getString("configName");
                    assertTrue(configName.toLowerCase().contains("deletedrecords"),
                          "Config name should contain 'deletedRecords': " + configName);
                  });
            testContext.completeNow();
          })));
  }

  @Test
  void shouldReturnAllConfigsWhenNameIsNull(VertxTestContext testContext) {
    String uniquePrefix = UUID.randomUUID().toString().substring(0, 8);
    Future<JsonObject> future1 = configurationSettingsDao.saveConfigurationSettings(
          createConfigWithName("test1." + uniquePrefix), OAI_TEST_TENANT, TEST_USER_ID);
    Future<JsonObject> future2 = configurationSettingsDao.saveConfigurationSettings(
          createConfigWithName("test2." + uniquePrefix), OAI_TEST_TENANT, TEST_USER_ID);

    Future.all(future1, future2)
          .compose(v -> configurationSettingsDao
            .getConfigurationSettingsList(0, 100, null, OAI_TEST_TENANT))
          .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
            assertNotNull(result);
            assertTrue(result.getInteger("totalRecords") >= 2);
            testContext.completeNow();
          })));
  }

  @Test
  void shouldReturnEmptyListWhenNoMatchingName(VertxTestContext testContext) {
    String uniqueSearch = "nonexistent-config-" + UUID.randomUUID();
    
    configurationSettingsDao
          .getConfigurationSettingsList(0, 10, uniqueSearch, OAI_TEST_TENANT)
          .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
            assertNotNull(result);
            assertTrue(result.getInteger("totalRecords") == 0);
            JsonArray configs = result.getJsonArray("configurationSettings");
            assertNotNull(configs);
            assertTrue(configs.isEmpty());
            testContext.completeNow();
          })));
  }

  @Test
  void shouldPerformCaseInsensitiveSearch(VertxTestContext testContext) {
    String uniquePrefix = UUID.randomUUID().toString().substring(0, 8);
    Future<JsonObject> future = configurationSettingsDao.saveConfigurationSettings(
          createConfigWithName("TestCaseInsensitive." + uniquePrefix), OAI_TEST_TENANT, TEST_USER_ID);

    future.compose(v -> configurationSettingsDao
            .getConfigurationSettingsList(0, 10, "testcaseinsensitive", OAI_TEST_TENANT))
          .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
            assertNotNull(result);
            assertTrue(result.getInteger("totalRecords") >= 1);
            JsonArray configs = result.getJsonArray("configurationSettings");
            assertNotNull(configs);
            boolean found = configs.stream()
                  .map(obj -> (JsonObject) obj)
                  .anyMatch(config -> config.getString("configName").contains(uniquePrefix));
            assertTrue(found, "Should find config with case-insensitive search");
            testContext.completeNow();
          })));
  }

  private JsonObject createConfigWithName(String configName) {
    return new JsonObject()
          .put("id", UUID.randomUUID().toString())
          .put("configName", configName)
          .put("configValue", new JsonObject()
                .put("value", "test-value"));
  }
}
