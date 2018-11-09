package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.folio.rest.RestVerticle;
import org.folio.rest.impl.OaiPmhImpl;
import org.folio.rest.impl.OkapiMockServer;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static org.folio.oaipmh.Constants.OKAPI_TENANT_HEADER;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN_HEADER;
import static org.folio.oaipmh.Constants.OKAPI_URL_HEADER;
import static org.folio.oaipmh.Constants.REPOSITORY_ADMIN_EMAILS;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_NAME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
class RepositoryConfigurationHelperTest {

  private static final Logger logger = LoggerFactory.getLogger(RepositoryConfigurationHelperTest.class);

  private static final int mockPort = NetworkUtils.nextFreePort();
  private static final int okapiPort = NetworkUtils.nextFreePort();

  private static final Map<String, String> okapiHeaders = new HashMap<>();
  private RepositoryConfigurationHelper helper;

  @BeforeAll
  static void setUpOnce(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TOKEN_HEADER, "eyJhbGciOiJIUzI1NiJ9");
    okapiHeaders.put(OKAPI_URL_HEADER, "http://localhost:" + mockPort);
    OkapiMockServer okapiMockServer = new OkapiMockServer(vertx, mockPort);
    JsonObject conf = new JsonObject()
      .put("http.port", okapiPort);
    DeploymentOptions opt = new DeploymentOptions().setConfig(conf);

    vertx.deployVerticle(RestVerticle.class.getName(), opt, testContext.succeeding(id ->
      OaiPmhImpl.init(testContext.succeeding(success -> {
        logger.info("mod-oai-pmh Test: setup done.");
        okapiMockServer.start(testContext);
      }))
    ));
  }

  @BeforeEach
  void init() {
    helper = new RepositoryConfigurationHelper();
  }

  @Test
  void testGetConfigurationIfExist(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT_HEADER, OkapiMockServer.EXIST_CONFIG_TENANT);
    vertx.deployVerticle(RestVerticle.class.getName(), testContext.succeeding(s ->

      helper.getConfiguration(okapiHeaders, Vertx.currentContext()).thenAccept(v ->
        testContext.verify(() -> {
          Context context = Vertx.currentContext();
          assertThat(RepositoryConfigurationHelper.getProperty(REPOSITORY_NAME, context),
            is(equalTo("FOLIO_OAI_Repository_mock")));
          assertThat(RepositoryConfigurationHelper.getProperty(REPOSITORY_BASE_URL, context), is(equalTo
            ("http://mock.folio.org/oai")));
          assertThat(RepositoryConfigurationHelper.getProperty(REPOSITORY_ADMIN_EMAILS, context), is(equalTo
            ("oai-pmh-admin1@folio.org")));
          assertThat(RepositoryConfigurationHelper.getProperty(REPOSITORY_MAX_RECORDS_PER_RESPONSE, context), is(equalTo
            ("100")));
          testContext.completeNow();
        })
      )
    ));
  }

  @Test
  void testGetConfigurationIfNotExist(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT_HEADER, OkapiMockServer.NON_EXIST_CONFIG_TENANT);
    vertx.deployVerticle(RestVerticle.class.getName(), testContext.succeeding(s ->
      helper.getConfiguration(okapiHeaders, Vertx.currentContext()).thenAccept(v ->
        testContext.verify(() -> {
          JsonObject config = Vertx.currentContext().config();
          assertThat(config, is(equalTo
            (new JsonObject())));
          testContext.completeNow();
        })
      )
    ));
  }

  @Test
  void testGetConfigurationIfUnexpectedStatusCode(Vertx vertx, VertxTestContext testContext) {
    okapiHeaders.put(OKAPI_TENANT_HEADER, OkapiMockServer.ERROR_TENANT);
    vertx.deployVerticle(RestVerticle.class.getName(), testContext.succeeding(s ->
      helper.getConfiguration(okapiHeaders, Vertx.currentContext()).thenAccept(v ->
        testContext.verify(() -> {
          JsonObject config = Vertx.currentContext().config();
          assertThat(config, is(equalTo
            (new JsonObject())));
          testContext.completeNow();
        })
      )
    ));
  }

  @Test
  void testConfigurationFallback(Vertx vertx, VertxTestContext testContext) {
    String expectedValue = "test value";
    System.setProperty(REPOSITORY_BASE_URL, expectedValue);
    vertx.deployVerticle(RestVerticle.class.getName(), testContext.succeeding(s ->
        testContext.verify(() -> {
          String propertyValue = RepositoryConfigurationHelper.getProperty(REPOSITORY_BASE_URL,
          Vertx.currentContext());
          assertThat(propertyValue, is(equalTo(expectedValue)));
          testContext.completeNow();
        })
      )
    );
  }

}
