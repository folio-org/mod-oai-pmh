package org.folio.oaipmh.service.impl;

import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.NotFoundException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.config.ApplicationConfig;
import org.folio.oaipmh.common.AbstractInstancesTest;
import org.folio.oaipmh.common.TestUtil;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.impl.OkapiMockServer;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.ModuleName;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.spring.SpringContextUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
class InstancesServiceImplTest extends AbstractInstancesTest {

  private final Logger logger = LogManager.getLogger(this.getClass());

  private static final String TEST_TENANT_ID = "oaiTest";
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final int EXPIRED_REQUEST_IDS_EMPTY_LIST_TIME = 2000;
  private static final int ZERO_EXPIRED_INSTANCES_TIME = INSTANCES_EXPIRATION_TIME_IN_SECONDS * 2;

  @Autowired
  private InstancesDao instancesDao;
  @Autowired
  private InstancesServiceImpl instancesService;

  @BeforeAll
  void setUpClass(Vertx vertx, VertxTestContext testContext) throws Exception {
    PostgresClientFactory.setShouldResetPool(true);
    logger.info("Test setup starting for {}.", ModuleName.getModuleName());
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient.getInstance(vertx, OAI_TEST_TENANT).startPostgresTester();
    Context context = vertx.getOrCreateContext();
    SpringContextUtil.init(vertx, context, ApplicationConfig.class);
    SpringContextUtil.autowireDependencies(this, context);
    new OkapiMockServer(vertx, mockPort).start(testContext);
    TestUtil.initializeTestContainerDbSchema(vertx, OAI_TEST_TENANT);
    testContext.completeNow();
  }

  @AfterAll
  static void tearDownClass(Vertx vertx, VertxTestContext testContext) {
    PostgresClientFactory.closeAll();
    vertx.close(testContext.succeeding(res -> {
      PostgresClient.stopPostgresTester();
      testContext.completeNow();
    }));
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
  void shouldReturnFutureWithExpiredIds_whenExpiredRequestIdsArePresented(VertxTestContext testContext) {
    testContext.verify(() -> instancesService.cleanExpiredInstances(TEST_TENANT_ID, EXPIRED_REQUEST_IDS_EMPTY_LIST_TIME)
      .onComplete(testContext.succeeding(ids -> {
        assertTrue(ids.contains(EXPIRED_REQUEST_ID));
        testContext.completeNow();
      })));
  }

  @Test
  void shouldSaveRequestMetadata(VertxTestContext testContext) {
    testContext.verify(() -> {
      UUID id = UUID.randomUUID();
      RequestMetadataLb requestMetadata = new RequestMetadataLb();
      requestMetadata.setRequestId(id);
      requestMetadata.setLastUpdatedDate(OffsetDateTime.now());
      instancesService.saveRequestMetadata(requestMetadata, OAI_TEST_TENANT)
        .onComplete(testContext.succeeding(requestMetadataLb -> {
          assertNotNull(requestMetadataLb.getRequestId());
          instancesService.deleteRequestMetadataByRequestId(id.toString(), OAI_TEST_TENANT).onComplete(testContext.succeeding(res -> {
            if (res) {
              testContext.completeNow();
            } else {
              testContext.failNow(new IllegalStateException("Cannot delete test request metadata with request id: " + id.toString()));
            }
          }));
        }));
    });
  }

  @Test
  void shouldUpdateRequestMetadataUpdatedDate_whenMetadataWithRequestIdExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      OffsetDateTime date = OffsetDateTime.now();
      requestMetadata.setLastUpdatedDate(date);
      instancesService.updateRequestUpdatedDate(requestMetadata.getRequestId().toString(), requestMetadata.getLastUpdatedDate(), OAI_TEST_TENANT).onComplete(testContext.succeeding(res -> {
        assertNotNull(res);
        testContext.completeNow();
      }));
    });
  }

  @Test
  void shouldReturnFailedFuture_whenUpdateRequestMetadataUpdatedDateWithRequestIdWhichDoesNotExist(VertxTestContext testContext) {
    testContext.verify(() ->
      instancesService.updateRequestUpdatedDate(nonExistentRequestMetadata.getRequestId().toString(),nonExistentRequestMetadata.getLastUpdatedDate(), OAI_TEST_TENANT).onComplete(testContext.failing(throwable -> {
        assertTrue(throwable instanceof NotFoundException);
        testContext.completeNow();
      }))
    );
  }

  @Test
  void shouldUpdateRequestMetadataStreamEnded_whenMetadataWithRequestIdExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      OffsetDateTime date = OffsetDateTime.now();
      requestMetadata.setLastUpdatedDate(date);
      instancesService.updateRequestStreamEnded(requestMetadata.getRequestId().toString(), true, OAI_TEST_TENANT).onComplete(testContext.succeeding(res -> {
        assertNotNull(res);
        testContext.completeNow();
      }));
    });
  }

  @Test
  void shouldReturnFailedFuture_whenUpdateRequestMetadataStreamEndedWithRequestIdWhichDoesNotExist(VertxTestContext testContext) {
    testContext.verify(() ->
      instancesService.updateRequestStreamEnded(nonExistentRequestMetadata.getRequestId().toString(), true, OAI_TEST_TENANT).onComplete(testContext.failing(throwable -> {
        assertTrue(throwable instanceof NotFoundException);
        testContext.completeNow();
      }))
    );
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
      instancesService.deleteInstancesById(instancesIds, REQUEST_ID, OAI_TEST_TENANT)
        .onSuccess(res -> {
          assertTrue(res);
          testContext.completeNow();
        })
        .onFailure(testContext::failNow);
    });
  }

  @Test
  void shouldReturnSucceedFutureWithFalseValue_whenDeleteInstancesByIdsAndSuchInstancesDoNotExist(VertxTestContext testContext) {
    testContext.verify(() -> {
      instancesService.deleteInstancesById(nonExistentInstancesIds, REQUEST_ID, OAI_TEST_TENANT)
        .onComplete(testContext.succeeding(res -> {
          assertFalse(res);
          testContext.completeNow();
        }));
    });
  }

  @Test
  void shouldReturnSucceedFutureWithFalseValue_whenDeletingExistentInstanceIdWithIncorrectRequestId(VertxTestContext testContext) {
    testContext.verify(() -> {
      String randomRequestId = UUID.randomUUID().toString();
      instancesService.deleteInstancesById(List.of(INSTANCE_ID), randomRequestId, OAI_TEST_TENANT).compose(res -> {
        assertFalse(res);
        return instancesService.getInstancesList(1, REQUEST_ID, OAI_TEST_TENANT);
      }).onSuccess(instanceIdList -> {
        assertEquals(1, instanceIdList.size());
        assertEquals(INSTANCE_ID, instanceIdList.get(0).getInstanceId().toString());
        testContext.completeNow();
      }).onFailure(testContext::failNow);
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
    testContext.verify(() -> instancesService.getInstancesList(100,  REQUEST_ID, OAI_TEST_TENANT)
      .onComplete(testContext.succeeding(instancesList -> {
        assertFalse(instancesList.isEmpty());
        testContext.completeNow();
      })));
  }

  @Test
  void shouldReturnSucceedFutureWithEmptyList_whenGetInstancesListAndThereNoAnyInstancesExist(VertxTestContext testContext) {
    testContext.verify(() -> cleanData().compose(res -> instancesService.getInstancesList(100, REQUEST_ID, OAI_TEST_TENANT))
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
