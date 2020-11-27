package org.folio.oaipmh.service.impl;

import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.commons.collections4.CollectionUtils;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.liquibase.SingleConnectionProvider;
import org.folio.oaipmh.common.AbstractInstancesTest;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.impl.OkapiMockServer;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
public class InstancesServiceImplTest extends AbstractInstancesTest {

  private static final String TEST_TENANT_ID = "oaiTest";
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final int EXPIRED_REQUEST_IDS_TIME = 1000;
  private static final int EXPIRED_REQUEST_IDS_EMPTY_LIST_TIME = 2000;
  private static final int EXPIRED_REQUEST_IDS_DAO_ERROR_TIME = 3000;
  private static final int ZERO_EXPIRED_INSTANCES_TIME = INSTANCES_EXPIRATION_TIME_IN_SECONDS * 2;

  private static final String REQUEST_ID_DAO_ERROR = "c75afb20-1812-45ab-badf-16d569502a99";
  private static final String REQUEST_ID_DAO_DB_SUCCESS_RESPONSE = "c86afb20-1812-45ab-badf-16d569502a99";

  private static List<String> validRequestIds = Collections.singletonList(REQUEST_ID_DAO_DB_SUCCESS_RESPONSE);
  private static List<String> daoErrorRequestId = Collections.singletonList(REQUEST_ID_DAO_ERROR);

  @Spy
  @Autowired
  private InstancesDao instancesDao;
  @Autowired
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

  @BeforeEach
  void setup(VertxTestContext testContext) {
    List<Future> futures = new ArrayList<>();
    requestMetadataList.forEach(elem -> futures.add(instancesDao.saveRequestMetadata(elem, OAI_TEST_TENANT)));
    futures.add(instancesDao.saveInstances(instancesList, OAI_TEST_TENANT));
    CompositeFuture.all(futures)
      .onSuccess(v -> testContext.completeNow())
      .onFailure(testContext::failNow);
  }

  @AfterEach
  void cleanUp(VertxTestContext testContext) {
    List<Future> futures = new ArrayList<>();
    requestIds.forEach(elem -> futures.add(instancesDao.deleteRequestMetadataByRequestId(elem, OAI_TEST_TENANT)));

    instancesDao.getInstancesList(0, 100, OAI_TEST_TENANT).onComplete(result -> {
      if (result.succeeded() && CollectionUtils.isNotEmpty(result.result())) {
        List<Instances> instances = result.result();
        List<String> instancesIds = instances.stream().map(Instances::getInstanceId).map(UUID::toString).collect(Collectors.toList());
        futures.add(instancesDao.deleteInstancesById(instancesIds, OAI_TEST_TENANT));
      } else {
        futures.add(Future.failedFuture(result.cause()));
      }
    });

    CompositeFuture.all(futures)
      .onSuccess(v -> testContext.completeNow())
      .onFailure(throwable -> {
        if (throwable instanceof NotFoundException) {
          testContext.completeNow();
        } else {
          testContext.failNow(throwable);
        }
      });
  }

  @Test
  void shouldReturnFutureWithEmptyList_whenThereNoExpiredRequestIds(VertxTestContext testContext) {
    testContext.verify(() -> instancesService.cleanExpiredInstances(TEST_TENANT_ID, ZERO_EXPIRED_INSTANCES_TIME)
      .onComplete(testContext.succeeding(ids -> {
        assertTrue(ids.isEmpty());
        testContext.completeNow();
      })));
  }

  @Test
  void shouldReturnFutureWithExpiredIds_whenThereExpiredRequestIdsArePresented(VertxTestContext testContext) {
    testContext.verify(() -> instancesService.cleanExpiredInstances(TEST_TENANT_ID, EXPIRED_REQUEST_IDS_EMPTY_LIST_TIME)
      .onComplete(testContext.succeeding(ids -> {
        assertTrue(ids.contains(REQUEST_ID_DAO_DB_SUCCESS_RESPONSE));
        testContext.completeNow();
      })));
  }

  @Test
  void shouldReturnFailedFuture_whenErrorOccurredInDao(VertxTestContext testContext) {
    when(instancesDao.deleteExpiredInstancesByRequestId(OAI_TEST_TENANT, anyList()))
      .thenThrow(new IllegalStateException("dao error"));
    testContext.verify(() -> instancesService.cleanExpiredInstances(TEST_TENANT_ID, EXPIRED_REQUEST_IDS_EMPTY_LIST_TIME)
      .onComplete(testContext.failing(throwable -> {
        assertEquals("dao error", throwable.getMessage());
        testContext.completeNow();
      })));
  }

  @Test
  void shouldSaveRequestMetadata(VertxTestContext testContext) {
    testContext.verify(() -> {
      instancesService.saveRequestMetadata(requestMetadata, OAI_TEST_TENANT)
        .onComplete(testContext.succeeding(requestMetadataLb -> {
          assertNotNull(requestMetadataLb.getRequestId());
          testContext.completeNow();
        }));
    });
  }

  @Test
  void shouldUpdateRequestMetadata_whenMetadataWithRequestIdExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      OffsetDateTime date = OffsetDateTime.now();
      requestMetadata.setLastUpdatedDate(date);
      instancesService.updateRequestMetadataByRequestId(requestMetadata.getRequestId().toString(), requestMetadata, OAI_TEST_TENANT).onComplete(testContext.succeeding(res -> {
        assertEquals(date, res.getLastUpdatedDate());
        testContext.completeNow();
      }));
    });
  }

  @Test
  void shouldReturnFailedFuture_whenUpdateRequestMetadataWithRequestIdWhichDoesNotExist(VertxTestContext testContext) {
    testContext.verify(() -> {
      instancesService.updateRequestMetadataByRequestId(nonExistentRequestMetadata.getRequestId().toString(), nonExistentRequestMetadata, OAI_TEST_TENANT).onComplete(testContext.failing(throwable -> {
        assertTrue(throwable instanceof NotFoundException);
        testContext.completeNow();
      }));
    });
  }

  @Test
  void shouldReturnFailedFuture_whenSaveRequestMetadataWithEmptyRequestId(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestMetadataLb requestMetadataLb = new RequestMetadataLb().setLastUpdatedDate(OffsetDateTime.now());
      instancesService.saveRequestMetadata(requestMetadataLb, OAI_TEST_TENANT).onComplete(testContext.failing(throwable -> {
        assertTrue(throwable instanceof IllegalStateException);
        testContext.completeNow();
      }));
    });
  }

  @Test
  void shouldReturnSucceededFuture_whenDeleteRequestMetadataByRequestIdAndSuchRequestMetadataExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      instancesService.deleteRequestMetadataByRequestId(REQUEST_ID, OAI_TEST_TENANT)
        .onComplete(testContext.succeeding(deleted -> {
          assertTrue(deleted);
          testContext.completeNow();
        }));
    });
  }

  @Test
  void shouldReturnFailedFuture_whenDeleteRequestMetadataByRequestIdAndSuchRequestMetadataDoesNotExist(
    VertxTestContext testContext) {
    testContext.verify(() -> {
      instancesService.deleteRequestMetadataByRequestId(NON_EXISTENT_REQUEST_ID, OAI_TEST_TENANT)
        .onComplete(testContext.failing(throwable -> {
          assertTrue(throwable instanceof NotFoundException);
          testContext.completeNow();
        }));
    });
  }

  @Test
  void shouldReturnSucceedFutureWithTrueValue_whenDeleteInstancesByIdsAndSuchInstancesExist(VertxTestContext testContext) {
    testContext.verify(() -> {
      instancesService.deleteInstancesById(instancesIds, OAI_TEST_TENANT)
        .onComplete(testContext.succeeding(res -> {
          assertTrue(res);
          testContext.completeNow();
        }));
    });
  }

  @Test
  void shouldReturnSucceedFutureWithFalseValue_whenDeleteInstancesByIdsAndSuchInstancesDoNotExist(VertxTestContext testContext) {
    testContext.verify(() -> {
      instancesService.deleteInstancesById(nonExistentInstancesIds, OAI_TEST_TENANT)
        .onComplete(testContext.succeeding(res -> {
          assertFalse(res);
          testContext.completeNow();
        }));
    });
  }

  @Test
  void shouldReturnSucceededFuture_whenSaveInstances(VertxTestContext testContext) {
    testContext.verify(() -> {
      instancesList.forEach(elem -> elem.setInstanceId(UUID.randomUUID()));
      instancesService.saveInstances(instancesList, OAI_TEST_TENANT)
        .onComplete(testContext.succeeding(res -> testContext.completeNow()));
    });
  }

  @Test
  void shouldReturnSucceedFutureWithInstancesList_whenGetInstancesListAndSomeInstancesExist(VertxTestContext testContext) {
    testContext.verify(() -> instancesService.getInstancesList(0, 100, OAI_TEST_TENANT)
      .onComplete(testContext.succeeding(instancesList -> assertFalse(instancesList.isEmpty()))));
  }

  @Test
  void shouldReturnSucceedFutureWithEmptyList_whenGetInstancesListAndThereNoAnyInstancesExist(VertxTestContext testContext) {
    testContext.verify(() -> instancesService.deleteInstancesById(instancesIds, OAI_TEST_TENANT)
      .compose(res -> instancesService.getInstancesList(0, 100, OAI_TEST_TENANT))
      .onComplete(testContext.succeeding(instancesList -> {
        assertTrue(instancesList.isEmpty());
        testContext.completeNow();
      })));
  }

  @Override
  protected InstancesDao getInstancesDao() {
    return instancesDao;
  }
}
