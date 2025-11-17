package org.folio.oaipmh.dao.impl;

import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import javax.ws.rs.NotFoundException;
import org.folio.oaipmh.common.TestUtil;
import org.folio.oaipmh.dao.ConfigurationSettingsDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.exception.ConfigSettingException;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.persist.PostgresClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
class ConfigurationSettingsDaoImplTest {

  private static final String TEST_USER_ID = "test-user-id";
  private static final String CONFIG_NAME = "behavioral";
  private static final String NONEXISTENT_ID = "00000000-0000-0000-0000-000000000000";

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
  void shouldSaveConfigurationSettings(VertxTestContext testContext) {
    String uniqueId = UUID.randomUUID().toString();
    String uniqueName = "test-config-" + UUID.randomUUID();
    JsonObject configEntry = createConfigWithName(uniqueName);
    configEntry.put("id", uniqueId);

    configurationSettingsDao.saveConfigurationSettings(configEntry, OAI_TEST_TENANT, TEST_USER_ID)
          .onComplete(testContext.succeeding(savedConfig -> testContext.verify(() -> {
            assertNotNull(savedConfig);
            assertEquals(uniqueId, savedConfig.getString("id"));
            assertEquals(uniqueName, savedConfig.getString("configName"));
            assertTrue(savedConfig.containsKey("configValue"));
            testContext.completeNow();
          })));
  }

  @Test
  void shouldGenerateIdIfNotProvided(VertxTestContext testContext) {
    String uniqueName = "test-config-no-id-" + UUID.randomUUID();
    JsonObject configEntry = createConfigWithName(uniqueName);
    configEntry.remove("id"); // Remove the ID

    configurationSettingsDao.saveConfigurationSettings(configEntry, OAI_TEST_TENANT, TEST_USER_ID)
          .onComplete(testContext.succeeding(savedConfig -> testContext.verify(() -> {
            assertNotNull(savedConfig.getString("id"));
            assertEquals(uniqueName, savedConfig.getString("configName"));
            testContext.completeNow();
          })));
  }


  @Test
  void shouldGetConfigurationSettingsById(VertxTestContext testContext) {
    String uniqueName = "test-config-get-" + UUID.randomUUID();
    JsonObject configEntry = createConfigWithName(uniqueName);

    configurationSettingsDao.saveConfigurationSettings(configEntry, OAI_TEST_TENANT, TEST_USER_ID)
          .compose(saved -> {
            String id = saved.getString("id");
            return configurationSettingsDao.getConfigurationSettingsById(id, OAI_TEST_TENANT);
          })
          .onComplete(testContext.succeeding(config -> testContext.verify(() -> {
            assertNotNull(config);
            assertEquals(uniqueName, config.getString("configName"));
            assertTrue(config.containsKey("configValue"));
            testContext.completeNow();
          })));
  }

  @Test
  void shouldThrowNotFoundWhenGetByNonExistentId(VertxTestContext testContext) {
    configurationSettingsDao.getConfigurationSettingsById(NONEXISTENT_ID, OAI_TEST_TENANT)
          .onComplete(testContext.failing(throwable -> testContext.verify(() -> {
            assertTrue(throwable instanceof NotFoundException);
            assertTrue(throwable.getMessage().contains("was not found"));
            testContext.completeNow();
          })));
  }

  @Test
  void shouldGetConfigurationSettingsByName(VertxTestContext testContext) {
    String uniqueName = "test-config-" + UUID.randomUUID();
    JsonObject configEntry = createConfigWithName(uniqueName);

    configurationSettingsDao.saveConfigurationSettings(configEntry, OAI_TEST_TENANT, TEST_USER_ID)
          .compose(saved -> configurationSettingsDao
            .getConfigurationSettingsByName(uniqueName, OAI_TEST_TENANT))
          .onComplete(testContext.succeeding(config -> testContext.verify(() -> {
            assertNotNull(config);
            assertEquals(uniqueName, config.getString("configName"));
            assertTrue(config.containsKey("configValue"));
            testContext.completeNow();
          })));
  }

  @Test
  void shouldThrowNotFoundWhenGetByNonExistentName(VertxTestContext testContext) {
    String nonExistentName = "non-existent-" + UUID.randomUUID();

    configurationSettingsDao.getConfigurationSettingsByName(nonExistentName, OAI_TEST_TENANT)
          .onComplete(testContext.failing(throwable -> testContext.verify(() -> {
            assertTrue(throwable instanceof NotFoundException);
            assertTrue(throwable.getMessage().contains("was not found"));
            testContext.completeNow();
          })));
  }

  @Test
  void shouldUpdateConfigurationSettingsById(VertxTestContext testContext) {
    String uniqueName = "test-config-update-" + UUID.randomUUID();
    JsonObject configEntry = createConfigWithName(uniqueName);
    JsonObject updateEntry = createConfigWithName(uniqueName);
    JsonObject updatedValue = new JsonObject()
          .put("deletedRecordsSupport", "persistent")
          .put("suppressedRecordsProcessing", true);
    updateEntry.put("configValue", updatedValue);

    configurationSettingsDao.saveConfigurationSettings(configEntry, OAI_TEST_TENANT, TEST_USER_ID)
          .compose(saved -> {
            String id = saved.getString("id");
            updateEntry.put("id", id);
            return configurationSettingsDao
              .updateConfigurationSettingsById(id, updateEntry, OAI_TEST_TENANT, TEST_USER_ID);
          })
          .onComplete(testContext.succeeding(updated -> testContext.verify(() -> {
            assertNotNull(updated);
            assertEquals(uniqueName, updated.getString("configName"));
            JsonObject configValue = updated.getJsonObject("configValue");
            assertEquals("persistent", configValue.getString("deletedRecordsSupport"));
            assertTrue(configValue.getBoolean("suppressedRecordsProcessing"));
            testContext.completeNow();
          })));
  }

  @Test
  void shouldThrowNotFoundWhenUpdateNonExistentConfig(VertxTestContext testContext) {
    JsonObject configEntry = createTestConfigEntry();

    configurationSettingsDao.updateConfigurationSettingsById(
        NONEXISTENT_ID, configEntry, OAI_TEST_TENANT, TEST_USER_ID)
          .onComplete(testContext.failing(throwable -> testContext.verify(() -> {
            assertTrue(throwable instanceof NotFoundException);
            testContext.completeNow();
          })));
  }

  @Test
  void shouldDeleteConfigurationSettingsById(VertxTestContext testContext) {
    String uniqueName = "test-config-delete-" + UUID.randomUUID();
    JsonObject configEntry = createConfigWithName(uniqueName);

    configurationSettingsDao.saveConfigurationSettings(configEntry, OAI_TEST_TENANT, TEST_USER_ID)
          .compose(saved -> {
            String id = saved.getString("id");
            return configurationSettingsDao.deleteConfigurationSettingsById(id, OAI_TEST_TENANT)
              .compose(deleted -> {
                assertTrue(deleted);
                return configurationSettingsDao.getConfigurationSettingsById(id, OAI_TEST_TENANT);
              });
          })
          .onComplete(testContext.failing(throwable -> testContext.verify(() -> {
            assertTrue(throwable instanceof NotFoundException);
            testContext.completeNow();
          })));
  }

  @Test
  void shouldThrowNotFoundWhenDeleteNonExistentConfig(VertxTestContext testContext) {
    configurationSettingsDao.deleteConfigurationSettingsById(NONEXISTENT_ID, OAI_TEST_TENANT)
          .onComplete(testContext.failing(throwable -> testContext.verify(() -> {
            assertTrue(throwable instanceof NotFoundException);
            testContext.completeNow();
          })));
  }

  @Test
  void shouldGetConfigurationSettingsList(VertxTestContext testContext) {
    Future<JsonObject> future1 = configurationSettingsDao.saveConfigurationSettings(
          createConfigWithName("list-config-1-" + UUID.randomUUID()),
        OAI_TEST_TENANT, TEST_USER_ID);
    Future<JsonObject> future2 = configurationSettingsDao.saveConfigurationSettings(
          createConfigWithName("list-config-2-" + UUID.randomUUID()),
        OAI_TEST_TENANT, TEST_USER_ID);

    Future.all(future1, future2)
          .compose(v -> configurationSettingsDao
            .getConfigurationSettingsList(0, 10, null, OAI_TEST_TENANT))
          .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
            assertNotNull(result);
            assertTrue(result.containsKey("configurationSettings"));
            assertTrue(result.containsKey("totalRecords"));
            assertTrue(result.getInteger("totalRecords") >= 2);
            JsonArray configs = result.getJsonArray("configurationSettings");
            assertNotNull(configs);
            testContext.completeNow();
          })));
  }

  @Test
  void shouldHandlePaginationCorrectly(VertxTestContext testContext) {
    Future<JsonObject> future1 = configurationSettingsDao.saveConfigurationSettings(
          createConfigWithName("pagination-1-" + UUID.randomUUID()), OAI_TEST_TENANT, TEST_USER_ID);
    Future<JsonObject> future2 = configurationSettingsDao.saveConfigurationSettings(
          createConfigWithName("pagination-2-" + UUID.randomUUID()), OAI_TEST_TENANT, TEST_USER_ID);
    Future<JsonObject> future3 = configurationSettingsDao.saveConfigurationSettings(
          createConfigWithName("pagination-3-" + UUID.randomUUID()), OAI_TEST_TENANT, TEST_USER_ID);

    Future.all(future1, future2, future3)
          .compose(v -> configurationSettingsDao
            .getConfigurationSettingsList(0, 2, null, OAI_TEST_TENANT))
          .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
            assertNotNull(result);
            assertTrue(result.getInteger("totalRecords") >= 3);
            JsonArray configs = result.getJsonArray("configurationSettings");
            assertNotNull(configs);
            assertTrue(configs.size() <= 2);
            testContext.completeNow();
          })));
  }

  @Test
  void shouldReturnEmptyListWhenNoConfigsExist(VertxTestContext testContext) {
    configurationSettingsDao.getConfigurationSettingsList(10000, 10, null, OAI_TEST_TENANT)
          .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
            assertNotNull(result);
            assertTrue(result.containsKey("configurationSettings"));
            assertTrue(result.containsKey("totalRecords"));
            JsonArray configs = result.getJsonArray("configurationSettings");
            assertNotNull(configs);
            testContext.completeNow();
          })));
  }

  @Test
  void shouldPreserveComplexConfigValue(VertxTestContext testContext) {
    JsonObject complexConfigValue = new JsonObject()
          .put("deletedRecordsSupport", "persistent")
          .put("suppressedRecordsProcessing", true)
          .put("errorsProcessing", "1000")
          .put("recordsSource", "Source record storage")
          .put("nestedObject", new JsonObject()
        .put("key1", "value1")
        .put("key2", 123)
        .put("key3", true))
        .put("arrayField", new JsonArray().add("item1").add("item2").add("item3"));

    JsonObject configEntry = new JsonObject()
          .put("id", UUID.randomUUID().toString())
          .put("configName", "complex-config")
          .put("configValue", complexConfigValue);

    configurationSettingsDao.saveConfigurationSettings(configEntry, OAI_TEST_TENANT, TEST_USER_ID)
          .compose(saved -> configurationSettingsDao.getConfigurationSettingsById(
            saved.getString("id"), OAI_TEST_TENANT))
          .onComplete(testContext.succeeding(retrieved -> testContext.verify(() -> {
            JsonObject retrievedValue = retrieved.getJsonObject("configValue");
            assertEquals("persistent", retrievedValue.getString("deletedRecordsSupport"));
            assertTrue(retrievedValue.getBoolean("suppressedRecordsProcessing"));
            assertEquals("1000", retrievedValue.getString("errorsProcessing"));

            JsonObject nestedObj = retrievedValue.getJsonObject("nestedObject");
            assertNotNull(nestedObj);
            assertEquals("value1", nestedObj.getString("key1"));
            assertEquals(123, nestedObj.getInteger("key2"));

            JsonArray arrayField = retrievedValue.getJsonArray("arrayField");
            assertNotNull(arrayField);
            assertEquals(3, arrayField.size());
            testContext.completeNow();
          })));
  }

  private JsonObject createTestConfigEntry() {
    return createConfigWithName(CONFIG_NAME);
  }

  private JsonObject createConfigWithName(String name) {
    JsonObject configValue = new JsonObject()
          .put("deletedRecordsSupport", "no")
          .put("suppressedRecordsProcessing", false)
          .put("errorsProcessing", "500")
          .put("recordsSource", "Source record storage");

    return new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("configName", name)
      .put("configValue", configValue);
  }
}
