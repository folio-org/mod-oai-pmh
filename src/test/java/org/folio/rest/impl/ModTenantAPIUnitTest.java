package org.folio.rest.impl;

import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.folio.config.ApplicationConfig;
import org.folio.oaipmh.common.TestUtil;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.spring.SpringContextUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ModTenantAPIUnitTest {

  private static final String TABLES_QUERY = "select * from pg_tables where schemaname='" + PostgresClient.convertToPsqlStandard(OAI_TEST_TENANT) + "'";
  private static final List<String> EXPECTED_TABLES = List.of("set_lb", "instances", "request_metadata_lb", "databasechangelog", "databasechangeloglock");

  private int okapiPort = -1;
  private ModTenantAPI modTenantAPI;
  private TenantAttributes tenantAttributes = new TenantAttributes().withModuleTo("99.99.99");

  @BeforeAll
  void beforeAll(Vertx vertx, VertxTestContext vtc) {
    var context = vertx.getOrCreateContext();
    SpringContextUtil.init(vertx, context, ApplicationConfig.class);
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient.getInstance(vertx, OAI_TEST_TENANT).startPostgresTester();
    vertx.runOnContext(v -> {
      modTenantAPI = new ModTenantAPI());
      startOkapiMockServer(vertx)
      .onComplete(vtc.succeedingThenComplete());
    });
  }

  @AfterAll
  void tearDown(Vertx vertx, VertxTestContext vtc) {
    vertx.close(res -> {
      if(res.succeeded()) {
        vtc.completeNow();
      } else {
        vtc.failNow(res.cause());
      }
    });
  }

  private Future<HttpServer> startOkapiMockServer(Vertx vertx) {
    return vertx.createHttpServer()
      .requestHandler(httpServerRequest -> {
        // mock mod-configuration responses
        if (httpServerRequest.method().equals(HttpMethod.POST)) {
          httpServerRequest.response()
            .setStatusCode(201)
            .end();
        } else {
          httpServerRequest.response()
            .setStatusCode(200)
            .end("{\"configs\":[]}");
        }
      })
      .listen(0)
      .onSuccess(httpServer -> {
        okapiPort = httpServer.actualPort();
        testContext.completeNow();
      });
  }

  private Map<String,String> headers() {
    var headers = new TreeMap<String,String>(String.CASE_INSENSITIVE_ORDER);
    headers.put("x-okapi-url", "http://localhost:" + okapiPort);
    headers.put("x-okapi-tenant", OAI_TEST_TENANT);
    return headers;
  }

  @Test
  void postTenantShouldSucceed(Vertx vertx, VertxTestContext vtc) {
    modTenantAPI.postTenant(tenantAttributes, headers(), vtc.succeedingThenComplete(), vertx.getOrCreateContext());
  }

  @Disabled
  @Test
  void postTenantShouldFail_whenNoTenantId(Vertx vertx, VertxTestContext vtc) {
    var headers = headers();
    headers.remove("x-okapi-tenant");
    modTenantAPI.postTenant(tenantAttributes, headers, vtc.failingThenComplete(), vertx.getOrCreateContext());
  }

  @Test
  void loadDataShouldSucceedAndDatabaseShouldBePopulated(Vertx vertx, VertxTestContext vtc) {
    TestUtil.prepareSchema(vertx, OAI_TEST_TENANT);
    modTenantAPI.loadData(tenantAttributes, OAI_TEST_TENANT, headers(), vertx.getOrCreateContext())
      .compose(v -> PostgresClient.getInstance(vertx, OAI_TEST_TENANT).select(TABLES_QUERY))
      .onSuccess(rows -> {
        assertEquals(5, rows.size());
        List<String> tables = new ArrayList<>();
        rows.forEach(row -> tables.add(row.getString("tablename")));
        assertTrue(tables.containsAll(EXPECTED_TABLES));
        vtc.completeNow();
      })
      .onFailure(vtc::failNow);
  }

}
