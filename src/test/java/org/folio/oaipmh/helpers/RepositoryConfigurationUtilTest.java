package org.folio.oaipmh.helpers;

import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.OKAPI_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_ADMIN_EMAILS;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_NAME;
import static org.folio.rest.impl.OkapiMockServer.ERROR_TENANT;
import static org.folio.rest.impl.OkapiMockServer.EXIST_CONFIG_TENANT;
import static org.folio.rest.impl.OkapiMockServer.EXIST_CONFIG_TENANT_2;
import static org.folio.rest.impl.OkapiMockServer.NON_EXIST_CONFIG_TENANT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.core.IsNull.nullValue;

import java.util.HashMap;
import java.util.Map;

import org.folio.oaipmh.Request;
import org.folio.rest.impl.OkapiMockServer;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
class RepositoryConfigurationUtilTest {

  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final Map<String, String> okapiHeaders = new HashMap<>();
  public static final String REPOSITORY_TEST_BOOLEAN_PROPERTY = "repository.testBooleanProperty";

  @BeforeAll
  static void setUpOnce(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TOKEN, "eyJhbGciOiJIUzI1NiJ9");
    OkapiMockServer okapiMockServer = new OkapiMockServer(vertx, mockPort);

    vertx.runOnContext(event -> testContext.verify(() -> okapiMockServer.start(testContext)));
  }

  @BeforeEach
  void init() {
    okapiHeaders.put(OKAPI_URL, "http://localhost:" + mockPort);
  }

  @Test
  void testGetConfigurationForDifferentTenantsIfExist(Vertx vertx, VertxTestContext testContext) {

    vertx.runOnContext(event -> {
      Map<String, Map<String, String>> tenantsWithExpectedConfigs = tenantAndExpectedConfigProvider();
      tenantsWithExpectedConfigs.keySet().forEach(tenant -> {
        okapiHeaders.put(OKAPI_TENANT, tenant);
        RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, Vertx.currentContext()).thenAccept(v ->
          testContext.verify(() -> {
            Map<String, String> expectedConfig = tenantsWithExpectedConfigs.get(tenant);
            expectedConfig.keySet().forEach(key -> assertThat(RepositoryConfigurationUtil.getProperty(tenant, key),
              is(equalTo(expectedConfig.get(key)))));
            testContext.completeNow();
          })
        );
      });
    });
  }

  @Test
  void testGetConfigurationIfNotExist(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT, NON_EXIST_CONFIG_TENANT);
    vertx.runOnContext(event ->
      RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, Vertx.currentContext()).thenAccept(v ->
        testContext.verify(() -> {
          assertThat(Vertx.currentContext().config().getJsonObject(NON_EXIST_CONFIG_TENANT), is(emptyIterable()));
          testContext.completeNow();
        })
      )
    );
  }

  @Test
  void testGetConfigurationIfUnexpectedStatusCode(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT, ERROR_TENANT);

    vertx.runOnContext(event ->
      RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, Vertx.currentContext()).thenAccept(v ->
        testContext.verify(() -> {
          assertThat(Vertx.currentContext().config().getJsonObject(ERROR_TENANT), is(nullValue()));
          testContext.completeNow();
        })
      ));
  }

  @Test
  void testGetConfigurationIfUnexpectedStatusCodeAndConfigAlreadyExist(Vertx vertx,
                                                                       VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT, ERROR_TENANT);
    String configValue = "123";
    vertx.runOnContext(event -> {
      JsonObject config = new JsonObject();
      config.put(REPOSITORY_MAX_RECORDS_PER_RESPONSE, configValue);
      Vertx.currentContext().config().put(ERROR_TENANT, config);
      RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, Vertx.currentContext()).thenAccept(v ->
        testContext.verify(() -> {
          assertThat(Vertx.currentContext().config().getJsonObject(ERROR_TENANT).getString
            (REPOSITORY_MAX_RECORDS_PER_RESPONSE), equalTo(configValue));
          testContext.completeNow();
        })
      );
    });
  }

  @Test
  void testGetConfigurationWithMissingOkapiHeader(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.remove(OKAPI_URL);

    vertx.runOnContext(event ->
      RepositoryConfigurationUtil.loadConfiguration(okapiHeaders, Vertx.currentContext()).thenAccept(v ->
        testContext.verify(() -> {
          assertThat(Vertx.currentContext().config(), is(emptyIterable()));
          testContext.completeNow();
        })
      ));
  }

  @Test
  void testConfigurationFallback(Vertx vertx, VertxTestContext testContext) {
    String expectedValue = "test value";
    System.setProperty(REPOSITORY_BASE_URL, expectedValue);
    vertx.runOnContext(event -> testContext.verify(() -> {
          String propertyValue = RepositoryConfigurationUtil.getProperty(NON_EXIST_CONFIG_TENANT, REPOSITORY_BASE_URL);
          assertThat(propertyValue, is(equalTo(expectedValue)));
          System.clearProperty(REPOSITORY_BASE_URL);
          testContext.completeNow();
        })
      );
  }

  @Test
  void testConfigurationGetBooleanProperty(Vertx vertx, VertxTestContext testContext) {
    boolean expectedValue = true;
    System.setProperty(REPOSITORY_TEST_BOOLEAN_PROPERTY, Boolean.toString(expectedValue));
    vertx.runOnContext(event -> testContext.verify(() ->{
      Map<String, String> okapiHeaders = new HashMap<>();
      okapiHeaders.put(OKAPI_TENANT, EXIST_CONFIG_TENANT);
      Request request = Request.builder().okapiHeaders(okapiHeaders).build();
        boolean propertyValue = RepositoryConfigurationUtil.getBooleanProperty(request, REPOSITORY_TEST_BOOLEAN_PROPERTY );
        assertThat(propertyValue, is(equalTo(expectedValue)));
        System.clearProperty(REPOSITORY_TEST_BOOLEAN_PROPERTY);
        testContext.completeNow();
      })
    );
  }

  private static Map<String, Map<String, String>> tenantAndExpectedConfigProvider() {
    Map<String, Map<String, String>> result = new HashMap<>();
    Map<String, String> existConfig = new HashMap<>();
    existConfig.put(REPOSITORY_NAME, "FOLIO_OAI_Repository_mock");
    existConfig.put(REPOSITORY_BASE_URL, "http://mock.folio.org/oai");
    existConfig.put(REPOSITORY_ADMIN_EMAILS, "oai-pmh-admin1@folio.org");
    existConfig.put(REPOSITORY_MAX_RECORDS_PER_RESPONSE, "100");
    result.put(EXIST_CONFIG_TENANT, existConfig);

    Map<String, String> existConfig2 = new HashMap<>();
    existConfig2.put(REPOSITORY_NAME, "FOLIO_OAI_Repository_mock");
    existConfig2.put(REPOSITORY_BASE_URL, "http://test.folio.org/oai");
    existConfig2.put(REPOSITORY_ADMIN_EMAILS, "oai-pmh-admin1@folio.org,oai-pmh-admin2@folio.org");
    existConfig2.put(REPOSITORY_MAX_RECORDS_PER_RESPONSE, "10");
    result.put(EXIST_CONFIG_TENANT_2, existConfig2);

    return result;
  }

}
