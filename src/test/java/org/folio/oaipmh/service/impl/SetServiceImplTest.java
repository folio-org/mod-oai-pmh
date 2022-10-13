package org.folio.oaipmh.service.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.pgclient.PgException;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.common.AbstractSetTest;
import org.folio.oaipmh.common.TestUtil;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.dao.SetDao;
import org.folio.oaipmh.dao.impl.SetDaoImpl;
import org.folio.oaipmh.service.SetService;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.impl.OkapiMockServer;
import org.folio.rest.jaxrs.model.FolioSet;
import org.folio.rest.jaxrs.model.FolioSetCollection;
import org.folio.rest.jaxrs.model.SetsFilteringCondition;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.ws.rs.NotFoundException;
import java.util.*;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static org.folio.oaipmh.Constants.*;
import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
@ExtendWith(VertxExtension.class)
class SetServiceImplTest extends AbstractSetTest {

  private static final String EXPECTED_NOT_FOUND_MSG = format("Set with id '%s' was not found", NONEXISTENT_SET_ID);
  private static final String EXPECTED_ITEM_WITH_ID_ALREADY_EXISTS_MSG = format("Set with id '%s' already exists",
      EXISTENT_SET_ID);
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static PostgresClientFactory postgresClientFactory;
  private SetService setService;

  @BeforeAll
  void setUpClass(Vertx vertx, VertxTestContext testContext) throws Exception {
    PostgresClientFactory.setShouldResetPool(true);
    postgresClientFactory = new PostgresClientFactory(vertx);
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient.getInstance(vertx, OAI_TEST_TENANT).startPostgresTester();
    SetDao setDao = new SetDaoImpl(postgresClientFactory);
    setService = new SetServiceImpl(setDao);
    TestUtil.initializeTestContainerDbSchema(vertx, OAI_TEST_TENANT);
    new OkapiMockServer(vertx, mockPort).start(testContext);
    dropSampleData(testContext);
    testContext.completeNow();
    WebClientProvider.init(vertx);
  }

  @AfterAll
  void tearDownClass(Vertx vertx, VertxTestContext testContext) {
    PostgresClientFactory.closeAll();
    WebClientProvider.closeAll();
    vertx.close(testContext.succeeding(res -> {
      testContext.completeNow();
    }));
  }

  @Test
  void shouldReturnSetItem_whenGetSetByIdAndSuchItemWithIdExists(VertxTestContext testContext) {
    setService.getSetById(EXISTENT_SET_ID, OAI_TEST_TENANT)
      .onComplete(result -> {
        if (result.failed()) {
          testContext.failNow(result.cause());
        }
        FolioSet set = result.result();
        verifyMainSetData(INITIAL_TEST_SET_ENTRY, set, true);
        verifyMetadata(set);
        testContext.completeNow();
      });
  }

  @Test
  void shouldThrowException_whenGetSetByIdAndSuchSetItemWithIdDoesNotExist(VertxTestContext testContext) {
    setService.getSetById(NONEXISTENT_SET_ID, OAI_TEST_TENANT)
      .onComplete(testContext.failing(throwable -> {
        assertTrue(throwable instanceof NotFoundException);
        assertEquals(EXPECTED_NOT_FOUND_MSG, throwable.getMessage());
        testContext.completeNow();
      }));
  }

  @Test
  void shouldReturnSetItemList(VertxTestContext testContext) {
    setService.getSetList(0, 100, OAI_TEST_TENANT)
      .onComplete(testContext.succeeding(setItemCollection -> {
        assertTrue(nonNull(setItemCollection.getSets()));
        assertEquals(1, setItemCollection.getTotalRecords().intValue());
        FolioSet set = setItemCollection.getSets()
          .iterator()
          .next();
        verifyMainSetData(INITIAL_TEST_SET_ENTRY, set, true);
        verifyMetadata(set);
        testContext.completeNow();
      }));
  }

  @Test
  void shouldUpdateAndReturnSetItem_whenUpdateSetByIdAndSuchSetItemWithIdExists(VertxTestContext testContext) {
    setService.updateSetById(EXISTENT_SET_ID, UPDATE_SET_ENTRY, OAI_TEST_TENANT, OkapiMockServer.TEST_USER_ID)
      .onComplete(result -> {
        if (result.failed()) {
          testContext.failNow(result.cause());
        }
        FolioSet updatedSet = result.result();
        verifyMainSetData(UPDATE_SET_ENTRY, updatedSet, true);
        verifyMetadata(updatedSet);
        testContext.completeNow();
      });
  }

  @Test
  void shouldNotUpdateSetItemAndThrowException_whenSuchSetItemWithIdDoesNotExist(VertxTestContext testContext) {
    setService.updateSetById(NONEXISTENT_SET_ID, UPDATE_SET_ENTRY, OAI_TEST_TENANT, OkapiMockServer.TEST_USER_ID)
      .onComplete(testContext.failing(throwable -> {
        assertTrue(throwable instanceof NotFoundException);
        assertEquals(EXPECTED_NOT_FOUND_MSG, throwable.getMessage());
        testContext.completeNow();
      }));
  }

  @Test
  void shouldSaveAndReturnSavedSetItem_whenSaveSet(VertxTestContext testContext) {
    var entry = new FolioSet().withName("new")
      .withDescription("new")
      .withSetSpec("new")
      .withId(UUID.randomUUID().toString());
    setService.saveSet(entry, OAI_TEST_TENANT, OkapiMockServer.TEST_USER_ID)
      .onComplete(result -> {
        if (result.failed()) {
          testContext.failNow(result.cause());
        }
        FolioSet savedSet = result.result();
        verifyMetadata(savedSet);
        testContext.completeNow();
      });
  }

  @Test
  void shouldNotSaveAndThrowException_whenSaveSetWithIdAndItemWithSuchIdAlreadyExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      POST_SET_ENTRY.setId(EXISTENT_SET_ID);
      setService.saveSet(POST_SET_ENTRY, OAI_TEST_TENANT, OkapiMockServer.TEST_USER_ID)
        .onComplete(testContext.failing(throwable -> {
          assertTrue(throwable instanceof IllegalArgumentException);
          assertEquals(EXPECTED_ITEM_WITH_ID_ALREADY_EXISTS_MSG, throwable.getMessage());
          POST_SET_ENTRY.setId(null);
          testContext.completeNow();
        }));
    });
  }

  @Test
  void shouldNotSaveItem_whenSaveItemWithAlreadyExistedSetSpecValue_CaseInsensitive(VertxTestContext testContext) {
    testContext.verify(() -> {
      setService.saveSet(POST_SET_ENTRY, OAI_TEST_TENANT, OkapiMockServer.TEST_USER_ID).onComplete(result -> {
        FolioSet setWithExistedSetSpecValue = result.result();
        setWithExistedSetSpecValue.setId(UUID.randomUUID().toString());
        setWithExistedSetSpecValue.setName("unique name");
        setWithExistedSetSpecValue.setSetSpec(setWithExistedSetSpecValue.getSetSpec().toUpperCase());
        setService.saveSet(setWithExistedSetSpecValue, OAI_TEST_TENANT, OkapiMockServer.TEST_USER_ID).onFailure(throwable -> {
          assertTrue(throwable instanceof PgException);
          String expectedErrorMessage = format(DUPLICATED_VALUE_DATABASE_ERROR_MSG, SET_SPEC_UNIQUE_CONSTRAINT);
          assertThat(throwable.getMessage(), containsString(expectedErrorMessage));
        });
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldNotSaveItem_whenSaveItemWithAlreadyExistedNameValue_CaseInsensitive(VertxTestContext testContext) {
    testContext.verify(() -> {
      setService.saveSet(POST_SET_ENTRY, OAI_TEST_TENANT, OkapiMockServer.TEST_USER_ID).onComplete(result -> {
        FolioSet setWithExistedNameValue = result.result();
        setWithExistedNameValue.setId(UUID.randomUUID().toString());
        setWithExistedNameValue.setSetSpec("unique setSpec");
        setWithExistedNameValue.setName(setWithExistedNameValue.getName().toUpperCase());
        setService.saveSet(setWithExistedNameValue, OAI_TEST_TENANT, OkapiMockServer.TEST_USER_ID).onFailure(throwable -> {
          assertTrue(throwable instanceof PgException);
          String expectedErrorMessage = format(DUPLICATED_VALUE_DATABASE_ERROR_MSG, NAME_UNIQUE_CONSTRAINT);
          assertThat(throwable.getMessage(), containsString(expectedErrorMessage));
        });
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldReturnTrue_whenDeleteSetByIdAndSuchItemWithIdExists(VertxTestContext testContext) {
    setService.deleteSetById(EXISTENT_SET_ID, OAI_TEST_TENANT)
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
    setService.deleteSetById(NONEXISTENT_SET_ID, OAI_TEST_TENANT)
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
    setDao.getSetList(0, 100, OAI_TEST_TENANT)
      .onComplete(result -> {
        FolioSetCollection setItemCollection = result.result();
        List<Future> futures = new ArrayList<>();
        setItemCollection.getSets()
          .forEach(setItem -> futures.add(setDao.deleteSetById(setItem.getId(), OAI_TEST_TENANT)));
        GenericCompositeFuture.all(futures)
          .onSuccess(compositeFuture -> testContext.completeNow())
          .onFailure(testContext::failNow);
      });
  }

  private void verifyContainsItem(String item, Collection<SetsFilteringCondition> verifiedCollection) {
    boolean res = verifiedCollection.stream()
      .anyMatch(colItem -> colItem.getName()
        .equals(item));
    assertTrue(res);
  }

  @Override
  protected SetService getSetService() {
    return setService;
  }

  @Override
  protected PostgresClientFactory getPostgresClientFactory() {
    return postgresClientFactory;
  }
}
