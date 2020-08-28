package org.folio.oaipmh.service.impl;

import static java.util.Objects.nonNull;
import static java.lang.String.format;
import static org.folio.oaipmh.Constants.ILL_POLICIES;
import static org.folio.oaipmh.Constants.INSTANCE_FORMATS;
import static org.folio.oaipmh.Constants.INSTANCE_TYPES;
import static org.folio.oaipmh.Constants.LOCATION;
import static org.folio.oaipmh.Constants.MATERIAL_TYPES;
import static org.folio.oaipmh.Constants.OKAPI_URL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.NotFoundException;

import org.folio.liquibase.LiquibaseUtil;
import org.folio.liquibase.SingleConnectionProvider;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.dao.SetDao;
import org.folio.oaipmh.dao.impl.SetDaoImpl;
import org.folio.oaipmh.service.SetService;
import org.folio.rest.impl.OkapiMockServer;
import org.folio.rest.jaxrs.model.FolioSet;
import org.folio.rest.jaxrs.model.FolioSetCollection;
import org.folio.rest.jaxrs.model.SetsFilteringCondition;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.pgclient.PgException;

@ExtendWith(VertxExtension.class)
class SetServiceImplTest {

  private static final String TEST_TENANT_ID = "oaiTest";
  private static final String EXISTENT_SET_ID = "16287799-d37a-49fb-ac8c-09e9e9fcbd4d";
  private static final String NONEXISTENT_SET_ID = "a3bd69dd-d50b-4aa6-accb-c1f9abaada55";
  private static final String TEST_USER_ID = "30fde4be-2d1a-4546-8d6c-b468caca2720";

  private static final String EXPECTED_NOT_FOUND_MSG = format("Set with id '%s' was not found", NONEXISTENT_SET_ID);
  private static final String EXPECTED_ITEM_WITH_ID_ALREADY_EXISTS_MSG = format("Set with id '%s' already exists",
      EXISTENT_SET_ID);
  private static final String DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_ERROR_MSG = "duplicate key value violates unique constraint \"%s\"";
  private static final String SET_SPEC_UNIQUE_CONSTRAINT = "set_spec_unique_constraint";
  private static final String NAME_UNIQUE_CONSTRAINT = "name_unique_constraint";

  private static final FolioSet INITIAL_TEST_SET_ENTRY = new FolioSet().withId(EXISTENT_SET_ID)
    .withName("test name")
    .withDescription("test description")
    .withSetSpec("test setSpec");

  private static final FolioSet UPDATE_SET_ENTRY = new FolioSet().withName("update name")
    .withDescription("update description")
    .withSetSpec("update SetSpec");

  private static final FolioSet POST_SET_ENTRY = new FolioSet().withName("post name")
    .withDescription("post description")
    .withSetSpec("post SetSpec");

  private static final int mockPort = NetworkUtils.nextFreePort();

  private static PostgresClientFactory postgresClientFactory;

  private SetDao setDao;
  private SetService setService;

  @BeforeAll
  static void setUpClass(Vertx vertx, VertxTestContext testContext) throws Exception {
    postgresClientFactory = new PostgresClientFactory(vertx);
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
    dropSampleData(testContext);
    testContext.completeNow();
  }

  @AfterAll
  static void tearDownClass(Vertx vertx, VertxTestContext context) {
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
    setDao.getSetList(0, 100, TEST_TENANT_ID).onSuccess(folioSetCollection -> {
      List<Future> list = new ArrayList<>();
      folioSetCollection.getSets().forEach(set -> {
        list.add(setDao.deleteSetById(set.getId(), TEST_TENANT_ID));
      });
      CompositeFuture.all(list).onComplete(result -> {
        if(result.failed()) {
          testContext.failNow(result.cause());
        } else {
          testContext.completeNow();
        }
      });
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
        assertTrue(nonNull(setItemCollection.getSets()));
        assertEquals(1, setItemCollection.getTotalRecords().intValue());
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
        testContext.completeNow();
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
  void shouldNotSaveItem_whenSaveItemWithAlreadyExistedSetSpecValue(VertxTestContext testContext) {
    testContext.verify(() -> {
      setService.saveSet(POST_SET_ENTRY, TEST_TENANT_ID, TEST_USER_ID).onComplete(result -> {
        FolioSet setWithExistedSetSpecValue = result.result();
        setWithExistedSetSpecValue.setId(UUID.randomUUID().toString());
        setWithExistedSetSpecValue.setName("unique name");
        setService.saveSet(setWithExistedSetSpecValue, TEST_TENANT_ID, TEST_USER_ID).onFailure(throwable -> {
          assertTrue(throwable instanceof PgException);
          assertEquals(format(DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_ERROR_MSG, SET_SPEC_UNIQUE_CONSTRAINT), throwable.getMessage());
          testContext.completeNow();
        });
      });
    });
  }

  @Test
  void shouldNotSaveItem_whenSaveItemWithAlreadyExistedNameValue(VertxTestContext testContext) {
    testContext.verify(() -> {
      setService.saveSet(POST_SET_ENTRY, TEST_TENANT_ID, TEST_USER_ID).onComplete(result -> {
        FolioSet setWithExistedNameValue = result.result();
        setWithExistedNameValue.setId(UUID.randomUUID().toString());
        setWithExistedNameValue.setSetSpec("unique setSpec");
        setService.saveSet(setWithExistedNameValue, TEST_TENANT_ID, TEST_USER_ID).onFailure(throwable -> {
          assertTrue(throwable instanceof PgException);
          assertEquals(format(DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_ERROR_MSG, NAME_UNIQUE_CONSTRAINT), throwable.getMessage());
          testContext.completeNow();
        });
      });
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

  @Test
  void shouldReturnFilteringConditions(VertxTestContext testContext) {
    Map<String, String> okapiHeaders = ImmutableMap.of(OKAPI_URL, "http://localhost:" + mockPort);
    List<String> expectedItems = ImmutableList.of(LOCATION, ILL_POLICIES, MATERIAL_TYPES, INSTANCE_TYPES, INSTANCE_FORMATS);
    setService.getFilteringConditions(okapiHeaders)
      .onComplete(testContext.succeeding(fkCollection -> {
        List<SetsFilteringCondition> fkValues = fkCollection.getSetsFilteringConditions();
        assertEquals(5, fkValues.size());
        expectedItems.forEach(item -> verifyContainsItem(item, fkValues));
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

  private void verifyContainsItem(String item, Collection<SetsFilteringCondition> verifiedCollection) {
    boolean res = verifiedCollection.stream().anyMatch(colItem -> colItem.getName().equals(item));
    assertTrue(res);
  }

}
