package org.folio.oaipmh.service.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.NotFoundException;

import org.folio.liquibase.LiquibaseUtil;
import org.folio.liquibase.SingleConnectionProvider;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.dao.SetDao;
import org.folio.oaipmh.dao.impl.SetDaoImpl;
import org.folio.oaipmh.service.SetService;
import org.folio.rest.jaxrs.model.FolioSet;
import org.folio.rest.jaxrs.model.FolioSetCollection;
import org.folio.rest.persist.PostgresClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
class SetServiceImplTest {

  private static final String TEST_TENANT_ID = "oaiTest";
  private static final String EXISTENT_SET_ID = "16287799-d37a-49fb-ac8c-09e9e9fcbd4d";
  private static final String NONEXISTENT_SET_ID = "a3bd69dd-d50b-4aa6-accb-c1f9abaada55";
  private static final String TEST_USER_ID = "30fde4be-2d1a-4546-8d6c-b468caca2720";

  private static final String EXPECTED_NOT_FOUND_MSG = String.format("Set with id '%s' was not found", NONEXISTENT_SET_ID);
  private static final String EXPECTED_ITEM_WITH_ID_ALREADY_EXISTS_MSG = String.format("Set with id '%s' already exists",
      EXISTENT_SET_ID);

  private static final FolioSet INITIAL_TEST_SET_ENTRY = new FolioSet().withId(EXISTENT_SET_ID)
    .withName("test name")
    .withDescription("test description")
    .withSetSpec("test setSpec");

  private static final FolioSet UPDATE_SET_ENTRY = new FolioSet().withName("update name")
    .withDescription("update description")
    .withSetSpec("update SetSpec");

  private static final FolioSet POST_SET_ENTRY = new FolioSet().withName("update name")
    .withDescription("update description")
    .withSetSpec("update SetSpec");

  private static PostgresClientFactory postgresClientFactory;
  private static Vertx vertx;

  private SetDao setDao;
  private SetService setService;

  @BeforeAll
  static void setUpClass(VertxTestContext testContext) throws Exception {
    vertx = Vertx.vertx();
    postgresClientFactory = new PostgresClientFactory(vertx);
    PostgresClient.getInstance(vertx)
      .startEmbeddedPostgres();

    try (Connection connection = SingleConnectionProvider.getConnection(vertx, TEST_TENANT_ID)) {
      connection.prepareStatement("create schema oaitest_mod_oai_pmh")
        .execute();
    } catch (Exception ex) {
      testContext.failNow(ex);
    }
    LiquibaseUtil.initializeSchemaForTenant(vertx, TEST_TENANT_ID);
    dropSampleData(testContext);
  }

  @AfterAll
  static void tearDownClass(VertxTestContext context) {
    PostgresClientFactory.closeAll();
    vertx.close(context.succeeding(res -> {
      PostgresClient.stopEmbeddedPostgres();
      context.completeNow();
    }));
  }

  @BeforeEach
  void setUp(VertxTestContext testContext) {
    setDao = new SetDaoImpl(postgresClientFactory);
    setService = new SetServiceImpl(setDao);
    loadTestData(testContext);
  }

  @AfterEach
  void cleanUp(VertxTestContext testContext) {
    setDao.deleteSetById(EXISTENT_SET_ID, TEST_TENANT_ID)
      .onComplete(result -> {
        testContext.completeNow();
      });
  }

  @Test
  void shouldReturnSetItem_whenGetSetByIdAndSuchItemWithIdExists(VertxTestContext testContext) {
    setService.getSetById(EXISTENT_SET_ID, TEST_TENANT_ID)
      .onComplete(result -> {
        if (result.failed()) {
          testContext.failNow(result.cause());
        }
        FolioSet set = result.result();
        verifyMainSetData(INITIAL_TEST_SET_ENTRY, set, true);
        assertEquals(TEST_USER_ID, set.getCreatedByUserId());
        assertNotNull(set.getCreatedDate());
        testContext.completeNow();
      });
  }

  @Test
  void shouldThrowException_whenGetSetByIdAndSuchSetItemWithIdDoesNotExist(VertxTestContext testContext) {
    setService.getSetById(NONEXISTENT_SET_ID, TEST_TENANT_ID)
      .onComplete(testContext.failing(throwable -> {
        assertTrue(throwable instanceof NotFoundException);
        assertEquals(EXPECTED_NOT_FOUND_MSG, throwable.getMessage());
        testContext.completeNow();
      }));
  }

  @Test
  void shouldReturnSetItemList(VertxTestContext testContext) {
    setService.getSetList(0, 100, TEST_TENANT_ID)
      .onComplete(testContext.succeeding(setItemCollection -> {
        assertEquals(1, setItemCollection.getSets()
          .size());
        FolioSet set = setItemCollection.getSets()
          .iterator()
          .next();
        verifyMainSetData(INITIAL_TEST_SET_ENTRY, set, true);
        testContext.completeNow();
      }));
  }

  @Test
  void shouldUpdateAndReturnSetItem_whenUpdateSetByIdAndSuchSetItemWithIdExists(VertxTestContext testContext) {
    setService.updateSetById(EXISTENT_SET_ID, UPDATE_SET_ENTRY, TEST_TENANT_ID, TEST_USER_ID)
      .onComplete(result -> {
        if (result.failed()) {
          testContext.failNow(result.cause());
        }
        FolioSet updatedSet = result.result();
        verifyMainSetData(UPDATE_SET_ENTRY, updatedSet, true);
        assertEquals(TEST_USER_ID, updatedSet.getUpdatedByUserId());
        assertEquals(TEST_USER_ID, updatedSet.getCreatedByUserId());
        assertNotNull(updatedSet.getCreatedDate());
        assertNotNull(updatedSet.getUpdatedDate());
        testContext.completeNow();
      });
  }

  @Test
  void shouldNotUpdateSetItemAndThrowException_whenSuchSetItemWithIdDoesNotExist(VertxTestContext testContext) {
    setService.updateSetById(NONEXISTENT_SET_ID, UPDATE_SET_ENTRY, TEST_TENANT_ID, TEST_USER_ID)
      .onComplete(testContext.failing(throwable -> {
        assertTrue(throwable instanceof NotFoundException);
        assertEquals(EXPECTED_NOT_FOUND_MSG, throwable.getMessage());
        testContext.completeNow();
      }));
  }

  @Test
  void shouldSaveAndReturnSavedSetItem_whenSaveSet(VertxTestContext testContext) {
    setService.saveSet(POST_SET_ENTRY, TEST_TENANT_ID, TEST_USER_ID)
      .onComplete(result -> {
        if (result.failed()) {
          testContext.failNow(result.cause());
        }
        FolioSet savedSet = result.result();
        verifyMainSetData(POST_SET_ENTRY, savedSet, false);
        assertEquals(TEST_USER_ID, savedSet.getCreatedByUserId());
        assertNotNull(savedSet.getCreatedDate());
        setDao.deleteSetById(savedSet.getId(), TEST_TENANT_ID)
          .onComplete(res -> {
            if (res.failed()) {
              testContext.failNow(res.cause());
            }
            testContext.completeNow();
          });
      });
  }

  @Test
  void shouldNotSaveAndThrowException_whenSaveSetWithIdAndItemWithSuchIdAlreadyExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      POST_SET_ENTRY.setId(EXISTENT_SET_ID);
      setService.saveSet(POST_SET_ENTRY, TEST_TENANT_ID, TEST_USER_ID)
        .onComplete(testContext.failing(throwable -> {
          assertTrue(throwable instanceof IllegalArgumentException);
          assertEquals(EXPECTED_ITEM_WITH_ID_ALREADY_EXISTS_MSG, throwable.getMessage());
          POST_SET_ENTRY.setId(null);
          testContext.completeNow();
        }));
    });
  }

  @Test
  void shouldReturnTrue_whenDeleteSetByIdAndSuchItemWithIdExists(VertxTestContext testContext) {
    setService.deleteSetById(EXISTENT_SET_ID, TEST_TENANT_ID)
      .onComplete(result -> {
        if (result.failed()) {
          testContext.failNow(result.cause());
        }
        assertTrue(result.result());
        testContext.completeNow();
      });
  }

  @Test
  void shouldThrowException_whenDeleteSetByIdAndItemWithSuchIdDoesNotExist(VertxTestContext testContext) {
    setService.deleteSetById(NONEXISTENT_SET_ID, TEST_TENANT_ID)
      .onComplete(testContext.failing(throwable -> {
        assertTrue(throwable instanceof NotFoundException);
        assertEquals(EXPECTED_NOT_FOUND_MSG, throwable.getMessage());
        testContext.completeNow();
      }));
  }

  private static void dropSampleData(VertxTestContext testContext) {
    SetDao setDao = new SetDaoImpl(postgresClientFactory);
    setDao.getSetList(0, 100, TEST_TENANT_ID)
      .onComplete(result -> {
        FolioSetCollection setItemCollection = result.result();
        List<Future> futures = new ArrayList<>();
        setItemCollection.getSets()
          .forEach(setItem -> futures.add(setDao.deleteSetById(setItem.getId(), TEST_TENANT_ID)));
        CompositeFuture.all(futures)
          .onSuccess(compositeFuture -> testContext.completeNow())
          .onFailure(testContext::failNow);
      });
  }

  private void loadTestData(VertxTestContext testContext) {
    setDao.saveSet(INITIAL_TEST_SET_ENTRY, TEST_TENANT_ID, TEST_USER_ID)
      .onComplete(result -> {
        if (result.failed()) {
          testContext.failNow(result.cause());
        }
        testContext.completeNow();
      });
  }

  private void verifyMainSetData(FolioSet setWithExpectedData, FolioSet setToVerify, boolean checkIdEquals) {
    assertEquals(setWithExpectedData.getName(), setToVerify.getName());
    assertEquals(setWithExpectedData.getDescription(), setToVerify.getDescription());
    assertEquals(setWithExpectedData.getSetSpec(), setToVerify.getSetSpec());
    if (checkIdEquals) {
      assertEquals(EXISTENT_SET_ID, setToVerify.getId());
    } else {
      assertNotNull(setToVerify.getId());
    }
  }

}
