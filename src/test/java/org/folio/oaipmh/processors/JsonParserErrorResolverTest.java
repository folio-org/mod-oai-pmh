package org.folio.oaipmh.processors;

import com.google.common.collect.ImmutableMap;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
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
import org.junit.jupiter.api.TestInstance;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

@ExtendWith(VertxExtension.class)
@TestInstance(PER_CLASS)
class JsonParserErrorResolverTest {

  private final Logger logger = LogManager.getLogger(this.getClass());

  private static Request request;

  @Autowired
  private ErrorService errorService;

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

  @ParameterizedTest
  @ValueSource(strings = {"instanceId", "some_field"})
  void testGetErrorPartAndErrorPosition(String instanceIdField) {
    var json = "{\"" + instanceIdField + "\":\"e54b1f4d-7d05-4b1a-9368-3c36b75d8ac6\",\"source\":\"FOLIO\"," +
      "\"holdings\":[{\"id\":\"e9285a1c-1dfc-4380-868c-e74073003f43\",\"notes\":[]," +
      "\"location\":{\"effectiveLocation\":{\"code\" \"KU/CC/DI/M\",\"name\":\"Main Library\"," +
      "\"campusName\":\"City Campus\"},\"permanentLocation\":{\"code\":\"KU/CC/DI/M\",\"name\":\"Main Library\"," +
      "\"campusName\":\"City Campus\",\"libraryName\":\"Datalogisk Institut\"}}" +
      ",\"formerIds\":[],\"callNumber\":{\"callNumber\":\"M1366.S67 T73 2017\"}," +
      "\"suppressFromDiscovery\":false, \"holdingsStatementsForSupplements\":[]}]}";

    int errorPositionByParser = 180;
    var errors = new ArrayList<String>();

    var jsonParser = new OaiPmhJsonParser(errorService, request)
      .objectValueMode().exceptionHandler(e -> errors.add(e.getLocalizedMessage()));
    jsonParser.write(Buffer.buffer(json)).end();

    var jsonParserErrorResolver = new JsonParserErrorResolver(json, errors.get(0));

    var expected = json.substring(errorPositionByParser, errorPositionByParser + 20);
    int errorPositionByParserErrorResolver = jsonParserErrorResolver.getErrorPosition();
    var actual = jsonParserErrorResolver.getErrorPart().substring(errorPositionByParserErrorResolver, errorPositionByParserErrorResolver + 20);

    assertEquals(expected, actual);
  }
}
