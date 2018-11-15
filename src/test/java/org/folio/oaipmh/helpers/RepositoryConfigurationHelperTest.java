package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.folio.rest.impl.OkapiMockServer;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.OKAPI_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_ADMIN_EMAILS;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_NAME;
import static org.folio.rest.impl.OkapiMockServer.NON_EXIST_CONFIG_TENANT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;

@ExtendWith(VertxExtension.class)
class RepositoryConfigurationHelperTest {

  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final Map<String, String> okapiHeaders = new HashMap<>();
  private RepositoryConfigurationHelper helper;

  @BeforeAll
  static void setUpOnce(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TOKEN, "eyJhbGciOiJIUzI1NiJ9");
    OkapiMockServer okapiMockServer = new OkapiMockServer(vertx, mockPort);

    vertx.runOnContext(event -> testContext.verify(() -> okapiMockServer.start(testContext)));
  }

  @BeforeEach
  void init() {
    okapiHeaders.put(OKAPI_URL, "http://localhost:" + mockPort);
    helper = new RepositoryConfigurationHelper();
  }

  @Test
  void testGetConfigurationIfExist(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT, OkapiMockServer.EXIST_CONFIG_TENANT);
    vertx.runOnContext(event ->
      helper.getConfiguration(okapiHeaders, Vertx.currentContext()).thenAccept(v ->
        testContext.verify(() -> {
          Context context = Vertx.currentContext();
          assertThat(RepositoryConfigurationHelper.getProperty(REPOSITORY_NAME, context),
            is(equalTo("FOLIO_OAI_Repository_mock")));
          assertThat(RepositoryConfigurationHelper.getProperty(REPOSITORY_BASE_URL, context), is(equalTo
            ("http://mock.folio.org/oai")));
          assertThat(RepositoryConfigurationHelper.getProperty(REPOSITORY_ADMIN_EMAILS, context), is(equalTo
            ("oai-pmh-admin1@folio.org")));
          assertThat(RepositoryConfigurationHelper.getProperty(REPOSITORY_MAX_RECORDS_PER_RESPONSE, context), is(equalTo("100")));
          testContext.completeNow();
        })
      )
    );
  }

  @Test
  void testGetConfigurationIfNotExist(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT, NON_EXIST_CONFIG_TENANT);
    vertx.runOnContext(event ->
      helper.getConfiguration(okapiHeaders, Vertx.currentContext()).thenAccept(v ->
        testContext.verify(() -> {
          assertThat(Vertx.currentContext().config().getJsonObject(NON_EXIST_CONFIG_TENANT), is(emptyIterable()));
          testContext.completeNow();
        })
      )
    );
  }

  @Test
  void testGetConfigurationIfUnexpectedStatusCode(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT, OkapiMockServer.ERROR_TENANT);

    vertx.runOnContext(event ->
      helper.getConfiguration(okapiHeaders, Vertx.currentContext()).thenAccept(v ->
        testContext.verify(() -> {
          assertThat(Vertx.currentContext().config(), is(emptyIterable()));
          testContext.completeNow();
        })
      ));
  }

  @Test
  void testGetConfigurationWithMissingOkapiHeader(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.remove(OKAPI_URL);

    vertx.runOnContext(event ->
      helper.getConfiguration(okapiHeaders, Vertx.currentContext()).thenAccept(v ->
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
          String propertyValue = RepositoryConfigurationHelper.getProperty(REPOSITORY_BASE_URL,
          Vertx.currentContext());
          assertThat(propertyValue, is(equalTo(expectedValue)));
          testContext.completeNow();
        })
      );
  }

}
