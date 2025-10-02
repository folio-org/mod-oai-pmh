package org.folio.oaipmh.helpers;

import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.OKAPI_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_ADMIN_EMAILS;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_NAME;
import static org.folio.rest.impl.OkapiMockServer.ERROR_TENANT;
import static org.folio.rest.impl.OkapiMockServer.EXIST_CONFIG_TENANT;
import static org.folio.rest.impl.OkapiMockServer.EXIST_CONFIG_TENANT_2;
import static org.folio.rest.impl.OkapiMockServer.INVALID_CONFIG_TENANT;
import static org.folio.rest.impl.OkapiMockServer.INVALID_JSON_TENANT;
import static org.folio.rest.impl.OkapiMockServer.NON_EXIST_CONFIG_TENANT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.json.DecodeException;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.HashMap;
import java.util.Map;
import org.folio.oaipmh.WebClientProvider;
import org.folio.rest.impl.OkapiMockServer;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class RepositoryConfigurationUtilTest {

  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final Map<String, String> okapiHeaders = new HashMap<>();
  private static final String REPOSITORY_TEST_BOOLEAN_PROPERTY = "repository.testBooleanProperty";

  @BeforeAll
  static void setUpOnce(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TOKEN, "eyJhbGciOiJIUzI1NiJ9");
    OkapiMockServer okapiMockServer = new OkapiMockServer(vertx, mockPort);

    vertx.runOnContext(event -> testContext.verify(() -> okapiMockServer.start(testContext)));
    WebClientProvider.init(vertx);
  }

  @BeforeEach
  void init() {
    okapiHeaders.put(OKAPI_URL, "http://localhost:" + mockPort);
  }

  @AfterAll
  static void afterAll() {
    WebClientProvider.closeAll();
  }

  @Test
  void testGetConfigurationForDifferentTenantsIfExist(Vertx vertx, VertxTestContext testContext) {
    vertx.runOnContext(event -> {
      Map<String, Map<String, String>> requestIdsWithExpectedConfigs =
          requestIdAndExpectedConfigProvider();
      requestIdsWithExpectedConfigs.keySet()
          .forEach(requestId -> {
            okapiHeaders.put(OKAPI_TENANT, requestId);
            RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, requestId)
            .onSuccess(v -> {
              Map<String, String> expectedConfig = requestIdsWithExpectedConfigs.get(requestId);
              expectedConfig.keySet().forEach(key ->
                  assertThat(RepositoryConfigurationUtil.getProperty(requestId, key),
                      is(equalTo(expectedConfig.get(key)))));
              testContext.completeNow();
            })
                .onFailure(testContext::failNow);
          });
    });
  }

  @Test
  void testGetConfigurationIfNotExist(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT, NON_EXIST_CONFIG_TENANT);
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

    vertx.runOnContext(event -> RepositoryConfigurationUtil.loadConfiguration(okapiHeaders,
          ERROR_TENANT)
        .onComplete(v -> {
          assertThat(RepositoryConfigurationUtil.getConfig(ERROR_TENANT), is(nullValue()));
          testContext.completeNow();
        }));
  }

  @Test
  void shouldReturnDefaultConfigValueWhenErrorResponseReturnedFromModConfig(Vertx vertx,
      VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT, ERROR_TENANT);
    String configValue = "123";
    System.setProperty(REPOSITORY_MAX_RECORDS_PER_RESPONSE, configValue);
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
  void testGetConfigurationWithMissingOkapiHeader(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.remove(OKAPI_URL);

    vertx.runOnContext(event -> testContext.verify(() -> {
      RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, "requestId")
          .onFailure(throwable -> {
            assertTrue(throwable instanceof VertxException);
            testContext.completeNow();
          }).onSuccess(v -> testContext.failNow(
            new IllegalStateException("An VertxException was expected to be thrown.")));
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

  @Test
  void shouldReturnDefaultConfigWhenInvalidConfigValueWasReturned(Vertx vertx,
      VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT, INVALID_CONFIG_TENANT);
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
  void shouldReturnFailedFuture_whenInvalidJsonReturnedFromModConfig2(Vertx vertx,
      VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT, INVALID_JSON_TENANT);
    vertx.runOnContext(event -> {
      RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, INVALID_JSON_TENANT)
          .onFailure(th -> {
            assertTrue(th instanceof DecodeException);
            testContext.completeNow();
          }).onSuccess(v -> testContext.failNow(
            new IllegalStateException("An DecodeException was expected to be thrown.")));
    });
  }
}
