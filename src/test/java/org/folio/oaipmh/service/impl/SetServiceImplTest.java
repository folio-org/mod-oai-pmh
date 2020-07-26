package org.folio.oaipmh.service.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.ws.rs.NotFoundException;

import org.folio.oaipmh.dao.SetDao;
import org.folio.oaipmh.dao.impl.SetDaoImpl;
import org.folio.oaipmh.service.SetService;
import org.folio.rest.jaxrs.model.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
class SetServiceImplTest extends AbstractServiceTest {

  private static final String EXISTENT_SET_ID = "16287799-d37a-49fb-ac8c-09e9e9fcbd4d";
  private static final String NONEXISTENT_SET_ID = "a3bd69dd-d50b-4aa6-accb-c1f9abaada55";
  private static final String TEST_USER_ID = "30fde4be-2d1a-4546-8d6c-b468caca2720";

  private static final String EXPECTED_NOT_FOUND_MSG = String.format("Set with id '%s' was not found", NONEXISTENT_SET_ID);
  private static final String EXPECTED_ITEM_WITH_ID_ALREADY_EXISTS_MSG = String.format("Set with id '%s' already exists",
      EXISTENT_SET_ID);

  private static final Set INITIAL_TEST_SET_ENTRY = new Set().withId(EXISTENT_SET_ID)
    .withName("test name")
    .withDescription("test description")
    .withSetSpec("test setSpec");

  private static final Set UPDATE_SET_ENTRY = new Set().withName("update name")
    .withDescription("update description")
    .withSetSpec("update SetSpec");

  private static final Set POST_SET_ENTRY = new Set().withName("update name")
    .withDescription("update description")
    .withSetSpec("update SetSpec");

  private SetDao setDao;
  private SetService setService;

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
        Set set = result.result();
        assertEquals(EXISTENT_SET_ID, set.getId());
        assertEquals(INITIAL_TEST_SET_ENTRY.getName(), set.getName());
        assertEquals(INITIAL_TEST_SET_ENTRY.getDescription(), set.getDescription());
        assertEquals(INITIAL_TEST_SET_ENTRY.getSetSpec(), set.getSetSpec());
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
  void shouldUpdateAndReturnSetItem_whenUpdateSetByIdAndSuchSetItemWithIdExists(VertxTestContext testContext) {
    setService.updateSetById(EXISTENT_SET_ID, UPDATE_SET_ENTRY, TEST_TENANT_ID, TEST_USER_ID)
      .onComplete(result -> {
        if (result.failed()) {
          testContext.failNow(result.cause());
        }
        Set updatedSet = result.result();
        assertEquals(EXISTENT_SET_ID, updatedSet.getId());
        assertEquals(UPDATE_SET_ENTRY.getName(), updatedSet.getName());
        assertEquals(UPDATE_SET_ENTRY.getDescription(), updatedSet.getDescription());
        assertEquals(UPDATE_SET_ENTRY.getSetSpec(), updatedSet.getSetSpec());
        assertEquals(TEST_USER_ID, updatedSet.getUpdatedByUserId());
        assertEquals(TEST_USER_ID, updatedSet.getCreatedByUserId());
        assertEquals(EXISTENT_SET_ID, updatedSet.getId());
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
        Set savedSet = result.result();
        assertNotNull(savedSet.getId());
        assertEquals(POST_SET_ENTRY.getName(), savedSet.getName());
        assertEquals(POST_SET_ENTRY.getDescription(), savedSet.getDescription());
        assertEquals(POST_SET_ENTRY.getSetSpec(), savedSet.getSetSpec());
        assertEquals(TEST_USER_ID, savedSet.getCreatedByUserId());
        assertNotNull(savedSet.getCreatedDate());
        testContext.completeNow();
      });
  }

  @Test
  void shouldNotSaveAndThrowException_whenSaveSetWithIdAndItemWithSuchIdAlreadyExists(VertxTestContext testContext) {
    POST_SET_ENTRY.setId(EXISTENT_SET_ID);
    setService.saveSet(POST_SET_ENTRY, TEST_TENANT_ID, TEST_USER_ID)
      .onComplete(testContext.failing(throwable -> {
        assertTrue(throwable instanceof IllegalArgumentException);
        assertEquals(EXPECTED_ITEM_WITH_ID_ALREADY_EXISTS_MSG, throwable.getMessage());
        POST_SET_ENTRY.setId(null);
        testContext.completeNow();
      }));
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

  private void loadTestData(VertxTestContext testContext) {
    setDao.saveSet(INITIAL_TEST_SET_ENTRY, TEST_TENANT_ID, TEST_USER_ID)
      .onComplete(result -> {
        if (result.failed()) {
          testContext.failNow(result.cause());
        }
        testContext.completeNow();
      });
  }

}
