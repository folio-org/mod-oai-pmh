package org.folio.oaipmh.common;

import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.folio.rest.jooq.tables.SetLb.SET_LB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableList;
import io.vertx.core.Future;
import io.vertx.junit5.VertxTestContext;
import java.util.List;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.service.SetService;
import org.folio.rest.impl.OkapiMockServer;
import org.folio.rest.jaxrs.model.FilteringCondition;
import org.folio.rest.jaxrs.model.FolioSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractSetTest {

  protected static final String EXISTENT_SET_ID = "16287799-d37a-49fb-ac8c-09e9e9fcbd4d";
  protected static final String NONEXISTENT_SET_ID = "a3bd69dd-d50b-4aa6-accb-c1f9abaada55";

  protected static final String DUPLICATED_VALUE_USER_ERROR_MSG =
      "Field '%s' cannot have duplicated values. Value '%s' is already taken."
      + " Please, pass another value";
  protected static final String DUPLICATED_VALUE_DATABASE_ERROR_MSG =
      "duplicate key value violates unique constraint \\\"%s\\\"";
  protected static final String SET_SPEC_UNIQUE_CONSTRAINT = "set_spec_unique_constraint";
  protected static final String NAME_UNIQUE_CONSTRAINT = "name_unique_constraint";

  private static FilteringCondition MOCK_FILTERING_CONDITION = new FilteringCondition()
      .withName("fk name")
      .withValue("fk value")
      .withSetSpec("fk setSpec");
  private static FilteringCondition MOCK_UPDATED_FILTERING_CONDITION = new FilteringCondition()
      .withName("fk updated name")
      .withValue("fk updated value")
      .withSetSpec("fk updated setSpec");

  private static List<FilteringCondition> mockFilteringConditions =
      ImmutableList.of(MOCK_FILTERING_CONDITION);
  private static List<FilteringCondition> mockUpdatedFilteringConditions =
      ImmutableList.of(MOCK_UPDATED_FILTERING_CONDITION);

  protected static FolioSet INITIAL_TEST_SET_ENTRY = new FolioSet().withId(EXISTENT_SET_ID)
      .withName("test name")
      .withDescription("test description")
      .withSetSpec("test setSpec")
      .withFilteringConditions(mockFilteringConditions);

  protected static FolioSet UPDATE_SET_ENTRY = new FolioSet()
      .withName("update name")
      .withDescription("update description")
      .withSetSpec("update SetSpec")
      .withFilteringConditions(mockUpdatedFilteringConditions);

  protected static FolioSet POST_SET_ENTRY = new FolioSet()
      .withName("update name")
      .withDescription("update description")
      .withSetSpec("update SetSpec")
      .withFilteringConditions(mockFilteringConditions);

  @BeforeEach
  void setUp(VertxTestContext testContext) {
    getSetService().saveSet(INITIAL_TEST_SET_ENTRY, OAI_TEST_TENANT, OkapiMockServer.TEST_USER_ID)
        .onComplete(result -> {
          if (result.failed()) {
            testContext.failNow(result.cause());
          }
          testContext.completeNow();
        });
  }

  @AfterEach
  protected void cleanTestData(VertxTestContext testContext) {
    deleteSets().onSuccess(v -> testContext.completeNow()).onFailure(testContext::failNow);
  }

  private Future<Integer> deleteSets() {
    return getPostgresClientFactory().getQueryExecutor(OAI_TEST_TENANT)
        .transaction(queryExecutor -> queryExecutor.execute(dslContext ->
            dslContext.deleteFrom(SET_LB)));
  }

  protected void verifyMainSetData(FolioSet setWithExpectedData, FolioSet setToVerify,
      boolean checkIdEquals) {
    assertEquals(setWithExpectedData.getName(), setToVerify.getName());
    assertEquals(setWithExpectedData.getDescription(), setToVerify.getDescription());
    assertEquals(setWithExpectedData.getSetSpec(), setToVerify.getSetSpec());
    if (checkIdEquals) {
      assertEquals(EXISTENT_SET_ID, setToVerify.getId());
    } else {
      assertNotNull(setToVerify.getId());
    }
    verifyFilteringConditions(setWithExpectedData.getFilteringConditions(),
        setToVerify.getFilteringConditions());
  }

  private void verifyFilteringConditions(List<FilteringCondition> fkWithExpectedData,
      List<FilteringCondition> fkList) {
    assertFalse(fkList.isEmpty());
    FilteringCondition expectedFk = fkWithExpectedData.iterator().next();
    FilteringCondition actualFk = fkList.iterator().next();
    assertEquals(expectedFk.getName(), actualFk.getName());
    assertEquals(expectedFk.getValue(), actualFk.getValue());
    assertEquals(expectedFk.getSetSpec(), actualFk.getSetSpec());
  }

  protected void verifyMetadata(FolioSet folioSet) {
    assertEquals(OkapiMockServer.TEST_USER_ID, folioSet.getCreatedByUserId());
    assertEquals(OkapiMockServer.TEST_USER_ID, folioSet.getUpdatedByUserId());
    assertNotNull(folioSet.getCreatedDate());
    assertNotNull(folioSet.getUpdatedDate());
  }

  protected abstract SetService getSetService();

  protected abstract PostgresClientFactory getPostgresClientFactory();

}

