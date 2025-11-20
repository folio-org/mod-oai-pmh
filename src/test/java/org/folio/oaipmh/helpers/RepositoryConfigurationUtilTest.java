package org.folio.oaipmh.helpers;

import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.REPOSITORY_ADMIN_EMAILS;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_NAME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.HashMap;
import java.util.Map;
import org.folio.oaipmh.service.ConfigurationSettingsService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class RepositoryConfigurationUtilTest {

  private static final Map<String, String> okapiHeaders = new HashMap<>();
  private static final String REPOSITORY_TEST_BOOLEAN_PROPERTY = "repository.testBooleanProperty";

  private static final String EXIST_CONFIG_TENANT = "test_diku";
  private static final String EXIST_CONFIG_TENANT_2 = "test_diku2";
  private static final String NON_EXIST_CONFIG_TENANT = "not_diku";
  private static final String ERROR_TENANT = "error";
  private static final String INVALID_CONFIG_TENANT = "invalid_config_value_tenant";
  private static final String INVALID_JSON_TENANT = "invalidJsonTenant";

  private ConfigurationSettingsService mockConfigService;

  @BeforeAll
  static void setUpOnce(Vertx vertx, VertxTestContext testContext) {
    testContext.completeNow();
  }

  @BeforeEach
  void init() {
    mockConfigService = mock(ConfigurationSettingsService.class);
    RepositoryConfigurationUtil.setConfigurationSettingsService(mockConfigService);
  }

  @Test
  void testGetConfigurationForDifferentTenantsIfExist(Vertx vertx, VertxTestContext testContext) {
    vertx.runOnContext(event -> {
      Map<String, Map<String, String>> requestIdsWithExpectedConfigs =
          requestIdAndExpectedConfigProvider();

      // Test first tenant
      String tenant1 = EXIST_CONFIG_TENANT;
      okapiHeaders.put(OKAPI_TENANT, tenant1);
      when(mockConfigService.getConfigurationSettingsList(anyInt(),
        anyInt(), isNull(), eq(tenant1)))
          .thenReturn(Future.succeededFuture(createConfigResponse(tenant1)));

      RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, tenant1)
        .onSuccess(v -> {
          Map<String, String> expectedConfig = requestIdsWithExpectedConfigs.get(tenant1);
          expectedConfig.keySet().forEach(key ->
              assertThat(RepositoryConfigurationUtil.getProperty(tenant1, key),
              is(equalTo(expectedConfig.get(key)))));

          // Test second tenant
          String tenant2 = EXIST_CONFIG_TENANT_2;
          okapiHeaders.put(OKAPI_TENANT, tenant2);
          when(mockConfigService.getConfigurationSettingsList(anyInt(),
            anyInt(), isNull(), eq(tenant2)))
              .thenReturn(Future.succeededFuture(createConfigResponse(tenant2)));

          RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, tenant2)
            .onSuccess(v2 -> {
              Map<String, String> expectedConfig2 = requestIdsWithExpectedConfigs.get(tenant2);
              expectedConfig2.keySet().forEach(key ->
                  assertThat(RepositoryConfigurationUtil.getProperty(tenant2, key),
                  is(equalTo(expectedConfig2.get(key)))));
              testContext.completeNow();
            })
              .onFailure(testContext::failNow);
        })
          .onFailure(testContext::failNow);
    });
  }

  @Test
  void testGetConfigurationIfNotExist(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT, NON_EXIST_CONFIG_TENANT);

    // Mock empty configuration response
    when(mockConfigService.getConfigurationSettingsList(anyInt(), anyInt(), isNull(),
        eq(NON_EXIST_CONFIG_TENANT)))
        .thenReturn(Future.succeededFuture(
          new JsonObject().put("configurationSettings", new JsonArray())));

    vertx.runOnContext(event -> RepositoryConfigurationUtil.loadConfiguration(okapiHeaders,
        NON_EXIST_CONFIG_TENANT)
        .onSuccess(v -> {
          assertThat(RepositoryConfigurationUtil.getConfig(NON_EXIST_CONFIG_TENANT),
              is(emptyIterable()));
          testContext.completeNow();
        })
        .onFailure(testContext::failNow));
  }

  @Test
  void testGetConfigurationIfUnexpectedStatusCode(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT, ERROR_TENANT);

    // Mock service failure
    when(mockConfigService.getConfigurationSettingsList(anyInt(), anyInt(), isNull(),
      eq(ERROR_TENANT)))
        .thenReturn(Future.failedFuture(new RuntimeException("Internal Server Error")));

    vertx.runOnContext(event -> RepositoryConfigurationUtil.loadConfiguration(okapiHeaders,
        ERROR_TENANT)
        .onComplete(v -> {
          assertThat(RepositoryConfigurationUtil.getConfig(ERROR_TENANT), is(nullValue()));
          testContext.completeNow();
        }));
  }

  @Test
  void shouldReturnDefaultConfigValueWhenErrorResponseReturnedFromModConfig(Vertx vertx,
                                                                            VertxTestContext
                                                                              testContext) {
    okapiHeaders.put(OKAPI_TENANT, ERROR_TENANT);
    String configValue = "123";
    System.setProperty(REPOSITORY_MAX_RECORDS_PER_RESPONSE, configValue);

    // Mock service failure
    when(mockConfigService.getConfigurationSettingsList(anyInt(), anyInt(), isNull(),
      eq(ERROR_TENANT)))
        .thenReturn(Future.failedFuture(new RuntimeException("Internal Server Error")));

    vertx.runOnContext(event -> {
      RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, ERROR_TENANT)
          .onComplete(result -> {
            assertThat(RepositoryConfigurationUtil.getProperty(ERROR_TENANT,
                REPOSITORY_MAX_RECORDS_PER_RESPONSE), equalTo(configValue));
            testContext.completeNow();
          });
    });
  }

  @Test
  void testGetConfigurationWithMissingConfigService(Vertx vertx, VertxTestContext testContext) {
    // Set config service to null to test error handling
    RepositoryConfigurationUtil.setConfigurationSettingsService(null);

    vertx.runOnContext(event -> testContext.verify(() -> {
      RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, "requestId")
          .onFailure(throwable -> {
            assertTrue(throwable instanceof IllegalStateException);
            testContext.completeNow();
          }).onSuccess(v -> testContext.failNow(
            new IllegalStateException("An IllegalStateException was expected to be thrown.")));
    }));
  }

  @Test
  void testConfigurationFallback(Vertx vertx, VertxTestContext testContext) {
    String expectedValue = "test value";
    System.setProperty(REPOSITORY_BASE_URL, expectedValue);
    vertx.runOnContext(event -> testContext.verify(() -> {
      String propertyValue = RepositoryConfigurationUtil.getProperty(NON_EXIST_CONFIG_TENANT,
          REPOSITORY_BASE_URL);
      assertThat(propertyValue, is(equalTo(expectedValue)));
      System.clearProperty(REPOSITORY_BASE_URL);
      testContext.completeNow();
    }));
  }

  @Test
  void testConfigurationGetBooleanProperty(Vertx vertx, VertxTestContext testContext) {
    boolean expectedValue = true;
    System.setProperty(REPOSITORY_TEST_BOOLEAN_PROPERTY, Boolean.toString(expectedValue));
    vertx.runOnContext(event -> testContext.verify(() -> {
      Map<String, String> okapiHeaders = new HashMap<>();
      okapiHeaders.put(OKAPI_TENANT, EXIST_CONFIG_TENANT);
      boolean propertyValue = RepositoryConfigurationUtil
          .getBooleanProperty(EXIST_CONFIG_TENANT, REPOSITORY_TEST_BOOLEAN_PROPERTY);
      assertThat(propertyValue, is(equalTo(expectedValue)));
      System.clearProperty(REPOSITORY_TEST_BOOLEAN_PROPERTY);
      testContext.completeNow();
    }));
  }

  private static Map<String, Map<String, String>> requestIdAndExpectedConfigProvider() {
    Map<String, String> existConfig = new HashMap<>();
    existConfig.put(REPOSITORY_NAME, "FOLIO_OAI_Repository_mock");
    existConfig.put(REPOSITORY_BASE_URL, "http://mock.folio.org/oai");
    existConfig.put(REPOSITORY_ADMIN_EMAILS, "oai-pmh-admin1@folio.org");
    existConfig.put(REPOSITORY_MAX_RECORDS_PER_RESPONSE, "100");
    Map<String, Map<String, String>> result = new HashMap<>();
    result.put(EXIST_CONFIG_TENANT, existConfig);

    Map<String, String> existConfig2 = new HashMap<>();
    existConfig2.put(REPOSITORY_NAME, "FOLIO_OAI_Repository_mock");
    existConfig2.put(REPOSITORY_BASE_URL, "http://test.folio.org/oai");
    existConfig2.put(REPOSITORY_ADMIN_EMAILS, "oai-pmh-admin1@folio.org,oai-pmh-admin2@folio.org");
    result.put(EXIST_CONFIG_TENANT_2, existConfig2);

    return result;
  }

  /**.
   * Helper method to create a configuration response for a given tenant
   */
  private JsonObject createConfigResponse(String tenant) {
    Map<String, Map<String, String>> configs = requestIdAndExpectedConfigProvider();
    Map<String, String> configForTenant = configs.get(tenant);

    if (configForTenant == null) {
      return new JsonObject().put("configurationSettings", new JsonArray());
    }
    JsonArray configsArray = new JsonArray();
    // Create a single configuration entry with all properties
    JsonObject configEntry = new JsonObject();
    JsonObject configValueObject = new JsonObject();
    configForTenant.forEach(configValueObject::put);
    configEntry.put("configValue", configValueObject);
    configEntry.put("configName", "test_config");
    configsArray.add(configEntry);
    JsonObject response = new JsonObject();

    response.put("configurationSettings", configsArray);
    return response;
  }

  @Test
  void shouldReturnDefaultConfigWhenInvalidConfigValueWasReturned(Vertx vertx,
                                                                  VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT, INVALID_CONFIG_TENANT);

    // Mock configuration with invalid value for deleted records
    JsonObject invalidConfig = new JsonObject();
    JsonArray configs = new JsonArray();
    JsonObject config = new JsonObject();
    config.put("configValue", new JsonObject()
        .put("deletedRecordsSupport", "invalidValue"));
    config.put("configName", "behavior");
    configs.add(config);
    invalidConfig.put("configurationSettings", configs);

    when(mockConfigService.getConfigurationSettingsList(anyInt(), anyInt(), isNull(),
      eq(INVALID_CONFIG_TENANT)))
        .thenReturn(Future.succeededFuture(invalidConfig));

    vertx.runOnContext(event -> {
      final String expectedValue = "true";
      System.setProperty(REPOSITORY_DELETED_RECORDS, expectedValue);
      RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, INVALID_CONFIG_TENANT)
        .onSuccess(v -> {
          final boolean deletedRecordsEnabled =
              RepositoryConfigurationUtil.isDeletedRecordsEnabled(INVALID_CONFIG_TENANT);
          assertThat(deletedRecordsEnabled, is(true));
          testContext.completeNow();
        }).onFailure(testContext::failNow);
    });
  }

  @Test
  void shouldReturnFailedFuture_whenInvalidJsonReturnedFromConfigService(Vertx vertx,
                                                                         VertxTestContext
                                                                           testContext) {
    okapiHeaders.put(OKAPI_TENANT, INVALID_JSON_TENANT);

    // Mock service returning invalid JSON that cannot be parsed
    when(mockConfigService.getConfigurationSettingsList(anyInt(), anyInt(), isNull(),
      eq(INVALID_JSON_TENANT)))
        .thenReturn(Future.failedFuture(
            new IllegalArgumentException("Invalid JSON structure")));

    vertx.runOnContext(event -> {
      RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, INVALID_JSON_TENANT)
          .onFailure(th -> {
            assertTrue(th instanceof IllegalStateException);
            testContext.completeNow();
          }).onSuccess(v -> testContext.failNow(
            new IllegalStateException("An exception was expected to be thrown.")));
    });
  }
}
