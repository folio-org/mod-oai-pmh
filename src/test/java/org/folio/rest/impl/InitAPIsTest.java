package org.folio.rest.impl;

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.MalformedURLException;

import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
class InitAPIsTest {

  private static final String TEST_PROP = "test.prop";

  @BeforeEach
  @AfterEach
  void setUp() {
    // Guarantee that properties defined in the 'config/config.properties' file are not available before test starts
    System.getProperties().remove(TEST_PROP);
    System.getProperties().remove(REPOSITORY_BASE_URL);
    // Reset to default config path
    System.getProperties().remove("configPath");
  }

  /**
   * Tests {@link InitAPIs#init(Vertx, Context, Handler)}
   */
  @Test
  void initWithFailure(Vertx vertx, VertxTestContext testContext) {
    // Specifying incorrect path to config file
    System.setProperty("configPath", "incorrectPath");

    new InitAPIs().init(vertx, vertx.getOrCreateContext(), testContext.failing(ex -> {
      assertThat(ex, instanceOf(IllegalStateException.class));
      assertThat(ex.getCause(), nullValue());
      assertThat(System.getProperty(TEST_PROP), nullValue());
      testContext.completeNow();
    }));
  }

  /**
   * Tests {@link InitAPIs#init(Vertx, Context, Handler)}
   */
  //@Test
  void initWithFailureIncorrectRepoBaseUrl(Vertx vertx, VertxTestContext testContext) {
    String invalid_url = "invalid_url";
    System.setProperty(REPOSITORY_BASE_URL, invalid_url);

    new InitAPIs().init(vertx, vertx.getOrCreateContext(), testContext.failing(ex -> {
      // Check that sys property was loaded from config file
      assertThat(System.getProperty(TEST_PROP), equalTo("Test Value"));
      // Check that "repository.baseURL" sys property wasn't loaded from config file as we specified it prior
      assertThat(System.getProperty(REPOSITORY_BASE_URL), equalTo(invalid_url));

      // Check that API failed to initialize successfully because valid repository baseURL is expected
      assertThat(ex, instanceOf(IllegalStateException.class));
      assertThat(ex.getCause(), instanceOf(MalformedURLException.class));
      testContext.completeNow();
    }));
  }

  /**
   * Tests {@link InitAPIs#init(Vertx, Context, Handler)}
   */
  @Test
  void initSuccess(Vertx vertx, VertxTestContext testContext) {
    new InitAPIs().init(vertx, vertx.getOrCreateContext(), testContext.succeeding(result -> {
      assertTrue(result);
      assertThat(System.getProperty(TEST_PROP), equalTo("Test Value"));
      testContext.completeNow();
    }));
  }
}
