package org.folio.rest.impl;

import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.folio.config.ApplicationConfig;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.common.TestUtil;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.spring.SpringContextUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ModTenantApiTest {

  private static final String TABLES_QUERY = "select * from pg_tables where schemaname='"
      + PostgresClient.convertToPsqlStandard(OAI_TEST_TENANT) + "'";
  private static final List<String> EXPECTED_TABLES = List.of("set_lb", "instances",
      "request_metadata_lb", "databasechangelog", "databasechangeloglock", "errors",
      "rmb_internal", "configuration_settings");

  private int okapiPort = -1;
  private ModTenantApi modTenantApi;
  private TenantAttributes tenantAttributes
      = new TenantAttributes().withModuleTo("mod-oai-pmh-99.99.99");

  @BeforeAll
  void beforeAll(Vertx vertx, VertxTestContext vtc) {
    var context = vertx.getOrCreateContext();

    SpringContextUtil.init(vertx, context, ApplicationConfig.class);
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient.getInstance(vertx, OAI_TEST_TENANT).startPostgresTester();
    WebClientProvider.init(vertx);

    context.runOnContext(v -> {
      try {
        modTenantApi = new ModTenantApi();
        TestUtil.prepareSchema(vertx, OAI_TEST_TENANT);
        TestUtil.prepareExternalTables(vertx, OAI_TEST_TENANT);
        TestUtil.prepareUser(vertx, OAI_TEST_TENANT, "username", "password");
      } catch (Exception e) {
        vtc.failNow(e);
        return;
      }
      startOkapiMockServer(vertx)
          .onComplete(vtc.succeedingThenComplete());
    });
  }

  @AfterAll
  void afterAll() {
    WebClientProvider.closeAll();
    PostgresClientFactory.closeAll();
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
        });
  }

  private Map<String, String> headers() {
    var headers = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    headers.put("x-okapi-url", "http://localhost:" + okapiPort);
    headers.put("x-okapi-tenant", OAI_TEST_TENANT);
    return headers;
  }

  @Test
  @Order(1)
  void postTenantShouldSucceedAndCreateDatabase(Vertx vertx, VertxTestContext vtc) {
    modTenantApi.postTenantSync(tenantAttributes, headers(), vtc.succeeding(r -> {
      assertEquals(204, r.getStatus());
      PostgresClient.getInstance(vertx, OAI_TEST_TENANT)
          .select(TABLES_QUERY)
          .compose(rows -> {
            List<String> tables = new ArrayList<>();
            rows.forEach(row -> tables.add(row.getString("tablename")));
            assertTrue(tables.containsAll(EXPECTED_TABLES));
            return Future.succeededFuture();
          })
          .andThen(vtc.succeedingThenComplete());
    }), vertx.getOrCreateContext());
  }

  @ParameterizedTest
  @NullAndEmptySource
  @Order(2)
  void postTenantShouldFailWhenNoOkapiUrl(String okapiUrl, Vertx vertx, VertxTestContext vtc) {
    var headers = headers();
    headers.put("x-okapi-url", okapiUrl);
    modTenantApi.postTenantSync(tenantAttributes, headers, vtc.succeeding(r -> {
      assertEquals(400, r.getStatus());
      vtc.completeNow();
    }), vertx.getOrCreateContext());
  }

  @Test
  @Order(3)
  void runAsyncShouldRetryOnTransientFailureAndSucceed(Vertx vertx, VertxTestContext vtc) {
    // Fail the first migration attempt, succeed on the retry.
    // runSqlFile call #0 = initial schema setup (files[0]), call #1 = first migration attempt.
    var api = new FailingModTenantApi(vertx, 1);
    api.postTenantSync(tenantAttributes, headers(), vtc.succeeding(r -> {
      assertEquals(204, r.getStatus());
      vtc.completeNow();
    }), vertx.getOrCreateContext());
  }

  @Test
  @Order(4)
  void runAsyncShouldFailAfterExhaustingAllRetries(Vertx vertx, VertxTestContext vtc) {
    // Fail more times than MAX_RETRIES (5) so all retries are exhausted.
    var api = new FailingModTenantApi(vertx, 6);
    api.postTenantSync(tenantAttributes, headers(), vtc.succeeding(r -> {
      assertEquals(400, r.getStatus());
      vtc.completeNow();
    }), vertx.getOrCreateContext());
  }

  /**
   * Test subclass that overrides the package-private {@code postgresClient(Context)} from
   * {@code TenantAPI} to inject a spy that fails {@code runSqlFile} for the first N migration
   * attempts. Call #0 to {@code runSqlFile} is the initial schema setup and is always allowed
   * through; calls #1..N are the migration attempts made by {@code runAsyncWithRetry}.
   */
  private class FailingModTenantApi extends ModTenantApi {

    private final Vertx vertx;
    private final int migrationCallsToFail;
    private final AtomicInteger runSqlFileCallCount = new AtomicInteger(0);
    private PostgresClient pgClientSpy;

    FailingModTenantApi(Vertx vertx, int migrationCallsToFail) {
      this.vertx = vertx;
      this.migrationCallsToFail = migrationCallsToFail;
    }

    @Override
    PostgresClient postgresClient(Context context) {
      if (pgClientSpy == null) {
        var real = PostgresClient.getInstance(vertx);
        pgClientSpy = spy(real);
        doAnswer(inv -> {
          int callNum = runSqlFileCallCount.getAndIncrement();
          // callNum 0: initial schema setup (files[0]) — always pass through
          // callNum 1..migrationCallsToFail: migration attempts — simulate failure
          // callNum > migrationCallsToFail: migration attempt — pass through
          if (callNum > 0 && callNum <= migrationCallsToFail) {
            return Future.failedFuture("Simulated transient RMB migration failure");
          }
          return real.runSqlFile(inv.getArgument(0));
        }).when(pgClientSpy).runSqlFile(any());
      }
      return pgClientSpy;
    }
  }
}
