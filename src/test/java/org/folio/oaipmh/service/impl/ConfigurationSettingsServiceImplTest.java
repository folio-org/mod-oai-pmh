package org.folio.oaipmh.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import javax.ws.rs.NotFoundException;
import org.folio.oaipmh.dao.ConfigurationSettingsDao;
import org.folio.oaipmh.service.ConfigurationSettingsService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class ConfigurationSettingsServiceImplTest {

  @Mock
  private ConfigurationSettingsDao configurationSettingsDao;

  private ConfigurationSettingsService configurationSettingsService;

  private static final String TEST_TENANT_ID = "test_tenant";
  private static final String TEST_USER_ID = "test_user";
  private static final String TEST_CONFIG_ID = "123e4567-e89b-12d3-a456-426614174000";
  private static final String TEST_CONFIG_NAME = "behavioral";

  private JsonObject testConfigurationSettings;
  private JsonObject testConfigurationSettingsList;

  @BeforeEach
  void setUp() {
    configurationSettingsService = new ConfigurationSettingsServiceImpl(configurationSettingsDao);

    testConfigurationSettings = new JsonObject()
      .put("id", TEST_CONFIG_ID)
      .put("configName", TEST_CONFIG_NAME)
      .put("configValue", new JsonObject()
        .put("deletedRecordsSupport", "no")
        .put("suppressedRecordsProcessing", false)
        .put("errorsProcessing", "500")
        .put("recordsSource", "Source record storage"));

    testConfigurationSettingsList = new JsonObject()
      .put("totalRecords", 1)
      .put("configurationSettings", new JsonArray().add(testConfigurationSettings));
  }

  @Test
  void shouldGetConfigurationSettingsById(VertxTestContext testContext) {
    given(configurationSettingsDao.getConfigurationSettingsById(TEST_CONFIG_ID, TEST_TENANT_ID))
          .willReturn(Future.succeededFuture(testConfigurationSettings));

    Future<JsonObject> future = configurationSettingsService
          .getConfigurationSettingsById(TEST_CONFIG_ID, TEST_TENANT_ID);

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertNotNull(result);
      assertEquals(TEST_CONFIG_ID, result.getString("id"));
      assertEquals(TEST_CONFIG_NAME, result.getString("configName"));
      verify(configurationSettingsDao).getConfigurationSettingsById(TEST_CONFIG_ID, TEST_TENANT_ID);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldFailWhenGetConfigurationSettingsByIdNotFound(VertxTestContext testContext) {
    given(configurationSettingsDao.getConfigurationSettingsById(TEST_CONFIG_ID, TEST_TENANT_ID))
          .willReturn(Future.failedFuture(
            new NotFoundException("Configuration setting not found")));

    Future<JsonObject> future = configurationSettingsService
          .getConfigurationSettingsById(TEST_CONFIG_ID, TEST_TENANT_ID);

    future.onComplete(testContext.failing(throwable -> testContext.verify(() -> {
      assertTrue(throwable instanceof NotFoundException);
      assertEquals("Configuration setting not found", throwable.getMessage());
      verify(configurationSettingsDao).getConfigurationSettingsById(TEST_CONFIG_ID, TEST_TENANT_ID);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldGetConfigurationSettingsByName(VertxTestContext testContext) {
    given(configurationSettingsDao.getConfigurationSettingsByName(TEST_CONFIG_NAME, TEST_TENANT_ID))
          .willReturn(Future.succeededFuture(testConfigurationSettings));

    Future<JsonObject> future = configurationSettingsService
          .getConfigurationSettingsByName(TEST_CONFIG_NAME, TEST_TENANT_ID);

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertNotNull(result);
      assertEquals(TEST_CONFIG_ID, result.getString("id"));
      assertEquals(TEST_CONFIG_NAME, result.getString("configName"));
      verify(configurationSettingsDao)
            .getConfigurationSettingsByName(TEST_CONFIG_NAME, TEST_TENANT_ID);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldFailWhenGetConfigurationSettingsByNameNotFound(VertxTestContext testContext) {
    given(configurationSettingsDao.getConfigurationSettingsByName(
          TEST_CONFIG_NAME, TEST_TENANT_ID)).willReturn(Future.failedFuture(
            new NotFoundException("Configuration setting not found")));

    Future<JsonObject> future = configurationSettingsService
          .getConfigurationSettingsByName(TEST_CONFIG_NAME, TEST_TENANT_ID);

    future.onComplete(testContext.failing(throwable -> testContext.verify(() -> {
      assertTrue(throwable instanceof NotFoundException);
      assertEquals("Configuration setting not found", throwable.getMessage());
      verify(configurationSettingsDao)
            .getConfigurationSettingsByName(TEST_CONFIG_NAME, TEST_TENANT_ID);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldSaveConfigurationSettings(VertxTestContext testContext) {
    given(configurationSettingsDao.saveConfigurationSettings(
      any(JsonObject.class), eq(TEST_TENANT_ID), eq(TEST_USER_ID)))
          .willReturn(Future.succeededFuture(testConfigurationSettings));

    Future<JsonObject> future = configurationSettingsService
          .saveConfigurationSettings(testConfigurationSettings, TEST_TENANT_ID, TEST_USER_ID);

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertNotNull(result);
      assertEquals(TEST_CONFIG_ID, result.getString("id"));
      assertEquals(TEST_CONFIG_NAME, result.getString("configName"));
      verify(configurationSettingsDao)
            .saveConfigurationSettings(testConfigurationSettings, TEST_TENANT_ID, TEST_USER_ID);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldFailWhenSaveConfigurationSettingsWithDuplicateId(VertxTestContext testContext) {
    given(configurationSettingsDao.saveConfigurationSettings(
          any(JsonObject.class), eq(TEST_TENANT_ID), eq(TEST_USER_ID)))
              .willReturn(Future.failedFuture(
                    new IllegalArgumentException("Configuration setting already exists")));

    Future<JsonObject> future = configurationSettingsService
          .saveConfigurationSettings(testConfigurationSettings, TEST_TENANT_ID, TEST_USER_ID);

    future.onComplete(testContext.failing(throwable -> testContext.verify(() -> {
      assertTrue(throwable instanceof IllegalArgumentException);
      assertEquals("Configuration setting already exists", throwable.getMessage());
      verify(configurationSettingsDao)
            .saveConfigurationSettings(testConfigurationSettings, TEST_TENANT_ID, TEST_USER_ID);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldUpdateConfigurationSettingsById(VertxTestContext testContext) {
    JsonObject updatedConfig = testConfigurationSettings.copy()
        .put("configValue", new JsonObject()
        .put("deletedRecordsSupport", "persistent")
        .put("suppressedRecordsProcessing", true));

    given(configurationSettingsDao.updateConfigurationSettingsById(
      eq(TEST_CONFIG_ID), any(JsonObject.class), eq(TEST_TENANT_ID), eq(TEST_USER_ID)))
          .willReturn(Future.succeededFuture(updatedConfig));

    Future<JsonObject> future = configurationSettingsService
          .updateConfigurationSettingsById(TEST_CONFIG_ID,
            updatedConfig, TEST_TENANT_ID, TEST_USER_ID);

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertNotNull(result);
      assertEquals(TEST_CONFIG_ID, result.getString("id"));
      assertEquals(TEST_CONFIG_NAME, result.getString("configName"));
      JsonObject configValue = result.getJsonObject("configValue");
      assertEquals("persistent", configValue.getString("deletedRecordsSupport"));
      assertTrue(configValue.getBoolean("suppressedRecordsProcessing"));
      verify(configurationSettingsDao)
            .updateConfigurationSettingsById(TEST_CONFIG_ID,
              updatedConfig, TEST_TENANT_ID, TEST_USER_ID);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldFailWhenUpdateConfigurationSettingsByIdNotFound(VertxTestContext testContext) {
    given(configurationSettingsDao.updateConfigurationSettingsById(
          eq(TEST_CONFIG_ID), any(JsonObject.class), eq(TEST_TENANT_ID), eq(TEST_USER_ID)))
              .willReturn(Future.failedFuture(
                new NotFoundException("Configuration setting not found")));

    Future<JsonObject> future = configurationSettingsService
          .updateConfigurationSettingsById(TEST_CONFIG_ID, testConfigurationSettings,
        TEST_TENANT_ID, TEST_USER_ID);

    future.onComplete(testContext.failing(throwable -> testContext.verify(() -> {
      assertTrue(throwable instanceof NotFoundException);
      assertEquals("Configuration setting not found", throwable.getMessage());
      verify(configurationSettingsDao)
            .updateConfigurationSettingsById(TEST_CONFIG_ID, testConfigurationSettings,
              TEST_TENANT_ID, TEST_USER_ID);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldDeleteConfigurationSettingsById(VertxTestContext testContext) {
    given(configurationSettingsDao.deleteConfigurationSettingsById(TEST_CONFIG_ID, TEST_TENANT_ID))
          .willReturn(Future.succeededFuture(true));

    Future<Boolean> future = configurationSettingsService
          .deleteConfigurationSettingsById(TEST_CONFIG_ID, TEST_TENANT_ID);

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertTrue(result);
      verify(configurationSettingsDao)
            .deleteConfigurationSettingsById(TEST_CONFIG_ID, TEST_TENANT_ID);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldFailWhenDeleteConfigurationSettingsByIdNotFound(VertxTestContext testContext) {
    given(configurationSettingsDao.deleteConfigurationSettingsById(
          TEST_CONFIG_ID, TEST_TENANT_ID)).willReturn(Future.failedFuture(
            new NotFoundException("Configuration setting not found")));

    Future<Boolean> future = configurationSettingsService
          .deleteConfigurationSettingsById(TEST_CONFIG_ID, TEST_TENANT_ID);

    future.onComplete(testContext.failing(throwable -> testContext.verify(() -> {
      assertTrue(throwable instanceof NotFoundException);
      assertEquals("Configuration setting not found", throwable.getMessage());
      verify(configurationSettingsDao)
            .deleteConfigurationSettingsById(TEST_CONFIG_ID, TEST_TENANT_ID);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldGetConfigurationSettingsList(VertxTestContext testContext) {
    int offset = 0;
    int limit = 10;
    given(configurationSettingsDao.getConfigurationSettingsList(offset, limit,
      null, TEST_TENANT_ID))
          .willReturn(Future.succeededFuture(testConfigurationSettingsList));

    Future<JsonObject> future = configurationSettingsService
          .getConfigurationSettingsList(offset, limit, null, TEST_TENANT_ID);

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertNotNull(result);
      assertEquals(1, result.getInteger("totalRecords"));
      JsonArray configArray = result.getJsonArray("configurationSettings");
      assertNotNull(configArray);
      assertEquals(1, configArray.size());
      JsonObject firstConfig = configArray.getJsonObject(0);
      assertEquals(TEST_CONFIG_ID, firstConfig.getString("id"));
      verify(configurationSettingsDao)
            .getConfigurationSettingsList(offset, limit, null, TEST_TENANT_ID);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldGetEmptyConfigurationSettingsList(VertxTestContext testContext) {
    int offset = 0;
    int limit = 10;
    JsonObject emptyList = new JsonObject()
          .put("totalRecords", 0)
          .put("configurationSettings", new JsonArray());

    given(configurationSettingsDao.getConfigurationSettingsList(offset, limit,
      null, TEST_TENANT_ID))
          .willReturn(Future.succeededFuture(emptyList));

    Future<JsonObject> future = configurationSettingsService
          .getConfigurationSettingsList(offset, limit, null, TEST_TENANT_ID);

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertNotNull(result);
      assertEquals(0, result.getInteger("totalRecords"));
      JsonArray configArray = result.getJsonArray("configurationSettings");
      assertNotNull(configArray);
      assertEquals(0, configArray.size());
      verify(configurationSettingsDao)
            .getConfigurationSettingsList(offset, limit, null, TEST_TENANT_ID);
      testContext.completeNow();
    })));
  }
}
