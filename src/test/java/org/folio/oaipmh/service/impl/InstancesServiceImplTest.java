package org.folio.oaipmh.service.impl;

import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.folio.rest.jooq.Tables.INSTANCES;
import static org.folio.rest.jooq.Tables.REQUEST_METADATA_LB;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.NotFoundException;

import org.folio.config.ApplicationConfig;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.oaipmh.common.AbstractInstancesTest;
import org.folio.oaipmh.common.TestUtil;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.impl.OkapiMockServer;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.spring.SpringContextUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class InstancesServiceImplTest extends AbstractInstancesTest {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private static final String TEST_TENANT_ID = "oaiTest";
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final int EXPIRED_REQUEST_IDS_EMPTY_LIST_TIME = 2000;
  private static final int ZERO_EXPIRED_INSTANCES_TIME = INSTANCES_EXPIRATION_TIME_IN_SECONDS * 2;

  private static final String REQUEST_ID_DAO_ERROR = "c75afb20-1812-45ab-badf-16d569502a99";
  private static final String REQUEST_ID_DAO_DB_SUCCESS_RESPONSE = "c86afb20-1812-45ab-badf-16d569502a99";

  private static List<String> validRequestIds = Collections.singletonList(REQUEST_ID_DAO_DB_SUCCESS_RESPONSE);
  private static List<String> daoErrorRequestId = Collections.singletonList(REQUEST_ID_DAO_ERROR);

  @Autowired
  private InstancesDao instancesDao;
  @Autowired
  private InstancesServiceImpl instancesService;

  @BeforeAll
  void setUpClass(Vertx vertx, VertxTestContext testContext) throws Exception {
    logger.info("Test setup starting for " + TestUtil.getModuleId());
    PostgresClient.getInstance(vertx)
      .startEmbeddedPostgres();
    TestUtil.prepareDatabase(vertx, testContext, OAI_TEST_TENANT, List.of(INSTANCES, REQUEST_METADATA_LB));
    Context context = vertx.getOrCreateContext();
    SpringContextUtil.init(vertx, context, ApplicationConfig.class);
    SpringContextUtil.autowireDependencies(this, context);
    new OkapiMockServer(vertx, mockPort).start(testContext);
    LiquibaseUtil.initializeSchemaForTenant(vertx, TEST_TENANT_ID);
    testContext.completeNow();
  }

  @AfterAll
  static void tearDownClass(Vertx vertx, VertxTestContext testContext) {
    PostgresClientFactory.closeAll();
    vertx.close(testContext.succeeding(res -> {
      PostgresClient.stopEmbeddedPostgres();
      testContext.completeNow();
    }));
  }

  @Order(1)
  @Test
  void shouldReturnFutureWithEmptyList_whenThereNoExpiredRequestIds(VertxTestContext testContext) {
    testContext.verify(() -> instancesService.cleanExpiredInstances(TEST_TENANT_ID, ZERO_EXPIRED_INSTANCES_TIME)
      .onComplete(testContext.succeeding(ids -> {
        assertTrue(ids.isEmpty());
        testContext.completeNow();
      })));
  }

  @Order(2)
  @Test
  void shouldReturnFutureWithExpiredIds_whenThereExpiredRequestIdsArePresented(VertxTestContext testContext) {
    testContext.verify(() -> instancesService.cleanExpiredInstances(TEST_TENANT_ID, EXPIRED_REQUEST_IDS_EMPTY_LIST_TIME)
      .onComplete(testContext.succeeding(ids -> {
        assertTrue(ids.contains(EXPIRED_REQUEST_ID));
        testContext.completeNow();
      })));
  }

  @Order(3)
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

  @Order(4)
  @Test
  void shouldUpdateRequestMetadata_whenMetadataWithRequestIdExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      OffsetDateTime date = OffsetDateTime.now();
      requestMetadata.setLastUpdatedDate(date);
      instancesService.updateRequestMetadataByRequestId(requestMetadata.getRequestId().toString(), requestMetadata, OAI_TEST_TENANT).onComplete(testContext.succeeding(res -> {
        assertNotNull(res);
        testContext.completeNow();
      }));
    });
  }

  @Order(5)
  @Test
  void shouldReturnFailedFuture_whenUpdateRequestMetadataWithRequestIdWhichDoesNotExist(VertxTestContext testContext) {
    testContext.verify(() ->
      instancesService.updateRequestMetadataByRequestId(nonExistentRequestMetadata.getRequestId().toString(), nonExistentRequestMetadata, OAI_TEST_TENANT).onComplete(testContext.failing(throwable -> {
        assertTrue(throwable instanceof NotFoundException);
        testContext.completeNow();
      }))
    );
  }

  @Order(6)
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

  @Order(7)
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

  @Order(8)
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

  @Order(9)
  @Test
  void shouldReturnSucceedFutureWithTrueValue_whenDeleteInstancesByIdsAndSuchInstancesExist(VertxTestContext testContext) {
    testContext.verify(() -> {
      instancesService.deleteInstancesById(instancesIds, OAI_TEST_TENANT)
        .onSuccess(res -> {
          assertTrue(res);
          testContext.completeNow();
        })
        .onFailure(testContext::failNow);
    });
  }

  @Order(10)
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

  @Order(11)
  @Test
  void shouldReturnSucceededFuture_whenSaveInstances(VertxTestContext testContext) {
    testContext.verify(() -> {
      instancesList.forEach(elem -> elem.setInstanceId(UUID.randomUUID()));
      instancesService.saveInstances(instancesList, OAI_TEST_TENANT)
        .onComplete(testContext.succeeding(res -> testContext.completeNow()));
    });
  }

  @Order(12)
  @Test
  void shouldReturnSucceedFutureWithInstancesList_whenGetInstancesListAndSomeInstancesExist(VertxTestContext testContext) {
    testContext.verify(() -> instancesService.getInstancesList(0, 100, OAI_TEST_TENANT)
      .onComplete(testContext.succeeding(instancesList -> {
        assertFalse(instancesList.isEmpty());
        testContext.completeNow();
      })));
  }

  @Order(13)
  @Test
  void shouldReturnSucceedFutureWithEmptyList_whenGetInstancesListAndThereNoAnyInstancesExist(VertxTestContext testContext) {
    testContext.verify(() -> cleanData().compose(res -> instancesService.getInstancesList(0, 100, OAI_TEST_TENANT))
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
