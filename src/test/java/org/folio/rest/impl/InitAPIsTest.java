package org.folio.rest.impl;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class InitAPIsTest {

  private static final String TEST_PROP = "test.prop";

  /**
   * Tests {@link InitAPIs#init(Vertx, Context, Handler)}
   */
  @Test
  public void init(TestContext context) {
    InitAPIs initAPIs = new InitAPIs();
    Vertx vertx = Vertx.vertx();
    Future<Boolean> resultHandler = Future.future();

    // Specifying incorrect path to config file
    System.setProperty("configPath", "incorrectPath");
    // Guarantee 'test.prop' is not available as Sys property. This property is defined in the 'config/config.properties' file
    System.getProperties().remove(TEST_PROP);
    // Verify failure case
    initAPIs.init(vertx, vertx.getOrCreateContext(), resultHandler);
    context
      .assertTrue(resultHandler.failed())
      .assertTrue(resultHandler.cause() instanceof IllegalStateException)
      .assertNull(System.getProperty(TEST_PROP));

    // Reset to default config path
    System.getProperties().remove("configPath");

    // Verify success case
    resultHandler = Future.future();
    initAPIs.init(vertx, vertx.getOrCreateContext(), resultHandler);
    context
      .assertTrue(resultHandler.result())
      .assertEquals("Test Value", System.getProperty(TEST_PROP));
  }
}
