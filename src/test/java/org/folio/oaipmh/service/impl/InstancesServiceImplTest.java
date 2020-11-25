package org.folio.oaipmh.service.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

import org.folio.liquibase.LiquibaseUtil;
import org.folio.liquibase.SingleConnectionProvider;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.impl.OkapiMockServer;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
public class InstancesServiceImplTest {

  private static final String TEST_TENANT_ID = "oaiTest";
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final int EXPIRED_REQUEST_IDS_TIME = 1000;
  private static final int EXPIRED_REQUEST_IDS_EMPTY_LIST_TIME = 2000;
  private static final int EXPIRED_REQUEST_IDS_DAO_ERROR_TIME = 3000;

  private static final String REQUEST_ID_DAO_ERROR = "c75afb20-1812-45ab-badf-16d569502a99";
  private static final String REQUEST_ID_DAO_DB_SUCCESS_RESPONSE = "c86afb20-1812-45ab-badf-16d569502a99";

  private static List<String> validRequestIds = Collections.singletonList(REQUEST_ID_DAO_DB_SUCCESS_RESPONSE);
  private static List<String> daoErrorRequestId = Collections.singletonList(REQUEST_ID_DAO_ERROR);

  @Mock
  private InstancesDao instancesDao;
  @Spy
  private InstancesServiceImpl instancesService;

  @BeforeAll
  static void setUpClass(Vertx vertx, VertxTestContext testContext) throws Exception {
    PostgresClient.getInstance(vertx)
      .startEmbeddedPostgres();

    try (Connection connection = SingleConnectionProvider.getConnection(vertx, TEST_TENANT_ID)) {
      connection.prepareStatement("create schema if not exists oaitest_mod_oai_pmh")
        .execute();
    } catch (Exception ex) {
      testContext.failNow(ex);
    }
    new OkapiMockServer(vertx, mockPort).start(testContext);
    LiquibaseUtil.initializeSchemaForTenant(vertx, TEST_TENANT_ID);
    testContext.completeNow();
  }

  private void mockData() {

  }

  @AfterAll
  static void tearDownClass(Vertx vertx, VertxTestContext testContext) {
    PostgresClientFactory.closeAll();
    vertx.close(testContext.succeeding(res -> {
      PostgresClient.stopEmbeddedPostgres();
      testContext.completeNow();
    }));
  }

  @Test
  void shouldReturnFutureWithEmptyList_whenThereNoExpiredRequestIds(VertxTestContext testContext) {
    when(instancesDao.getExpiredRequestIds(TEST_TENANT_ID, EXPIRED_REQUEST_IDS_EMPTY_LIST_TIME))
      .thenReturn(Future.succeededFuture(Collections.emptyList()));
    testContext.verify(() -> instancesService.cleanExpiredInstances(TEST_TENANT_ID, EXPIRED_REQUEST_IDS_EMPTY_LIST_TIME)
      .onComplete(testContext.succeeding(ids -> {
        assertTrue(ids.isEmpty());
        testContext.completeNow();
      })));
  }

  @Test
  void shouldReturnFutureWithExpiredIds_whenThereExpiredRequestIdsArePresented(VertxTestContext testContext) {
    when(instancesDao.getExpiredRequestIds(TEST_TENANT_ID, EXPIRED_REQUEST_IDS_TIME))
      .thenReturn(Future.succeededFuture(validRequestIds));
    when(instancesDao.deleteExpiredInstancesByRequestId(TEST_TENANT_ID, validRequestIds))
      .thenReturn(Future.succeededFuture(Boolean.TRUE));
    testContext.verify(() -> instancesService.cleanExpiredInstances(TEST_TENANT_ID, EXPIRED_REQUEST_IDS_EMPTY_LIST_TIME)
      .onComplete(testContext.succeeding(ids -> {
        assertTrue(ids.contains(REQUEST_ID_DAO_DB_SUCCESS_RESPONSE));
        testContext.completeNow();
      })));
  }

  @Test
  void shouldReturnFailedFuture_whenErrorOccurredInDao(VertxTestContext testContext) {
    when(instancesDao.getExpiredRequestIds(TEST_TENANT_ID, EXPIRED_REQUEST_IDS_DAO_ERROR_TIME))
      .thenReturn(Future.succeededFuture(daoErrorRequestId));
    when(instancesDao.deleteExpiredInstancesByRequestId(TEST_TENANT_ID, daoErrorRequestId))
      .thenReturn(Future.failedFuture(new Exception("dao error")));
    testContext.verify(() -> instancesService.cleanExpiredInstances(TEST_TENANT_ID, EXPIRED_REQUEST_IDS_EMPTY_LIST_TIME)
      .onComplete(testContext.failing(throwable -> {
        assertEquals("dao error", throwable.getMessage());
        testContext.completeNow();
      })));
  }

}
