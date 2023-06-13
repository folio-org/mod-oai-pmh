package org.folio.oaipmh.processors;

import com.google.common.collect.ImmutableMap;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.config.ApplicationConfig;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.service.ErrorService;
import org.folio.postgres.testing.PostgresTesterContainer;
import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.ModuleName;
import org.folio.spring.SpringContextUtil;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

@ExtendWith(VertxExtension.class)
@TestInstance(PER_CLASS)
class OaiPmhJsonParserTest {

  private final Logger logger = LogManager.getLogger(this.getClass());

  @Autowired
  private ErrorService errorService;

  private static Request request;

  @BeforeAll
  void setUpOnce(Vertx vertx) {
    Map<String, String> okapiHeaders = ImmutableMap.of("X-Okapi-Tenant", "diku");
    request = Request.builder().okapiHeaders(okapiHeaders).build();
    request.setRequestId(UUID.randomUUID().toString());
    PostgresClientFactory.setShouldResetPool(true);
    logger.info("Test setup starting for {}.", ModuleName.getModuleName());
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient.getInstance(vertx, OAI_TEST_TENANT).startPostgresTester();
    Context context = vertx.getOrCreateContext();
    SpringContextUtil.init(vertx, context, ApplicationConfig.class);
    SpringContextUtil.autowireDependencies(this, context);
  }

  @AfterAll
  static void tearDownClass() {
    PostgresClientFactory.closeAll();
  }

  @Test
 void testParserHandleValidData() {
    var instances = "{  \"instanceId\": \"e6bc03c6-c137-4221-b679-a7c5c31f986c\" , \"source\": \"FOLIO\", " +
      "\"updatedDate\": \"2020-06-15T11:07:48.563Z\",  \"deleted\": \"false\",  \"suppressFromDiscovery\": \"false\"}" +
      "\n\r { \"instanceId\" : \"a7bc91c6-c137-4221-b679-a7c5c31f986c\", \"source\": \"FOLIO\"," +
      " \"updatedDate\": \"2020-06-15T11:07:48.732Z\",  \"deleted\": \"false\",  \"suppressFromDiscovery\": \"false\"}";
    var errors = new ArrayList<>();
    var events = new ArrayList<JsonEvent>();

    var jsonParser = new OaiPmhJsonParser(errorService, request)
      .objectValueMode()
      .handler(events::add)
      .exceptionHandler(errors::add);
    jsonParser.write(Buffer.buffer(instances)).end();
    assertEquals(0, errors.size());
    assertEquals(2, events.size());
  }

  @Test
  void testParserHandleNotValidData() {
    var json = "{  \"instanceId\": \"e6bc03c6-c137-4221-b679-a7c5c31f986c\" , \"source\": \"FOLIO\", " +
      "\"updatedDate\": \"2020-06-15T11:07:48.563Z\",  \"deleted\": \"false\",  \"suppressFromDiscovery\": \"false\"}" +
      "\n\r { \"instanceId\"  \"a7bc91c6-c137-4221-b679-a7c5c31f986c\", \"source\": \"FOLIO\"," +
      " \"updatedDate\": \"2020-06-15T11:07:48.732Z\",  \"deleted\": \"false\" : \"suppressFromDiscovery\": \"false\"}";
    var errors = new ArrayList<>();
    var events = new ArrayList<JsonEvent>();

    var jsonParser = new OaiPmhJsonParser(errorService, request)
      .objectValueMode()
      .handler(events::add)
      .exceptionHandler(errors::add);
    jsonParser.write(Buffer.buffer(json)).end();
    assertEquals(2, errors.size());
    assertEquals(0, events.size());
  }
}
