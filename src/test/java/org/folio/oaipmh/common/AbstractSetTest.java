package org.folio.oaipmh.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.folio.rest.impl.OkapiMockServer;
import org.folio.rest.jaxrs.model.FilteringCondition;
import org.folio.rest.jaxrs.model.FolioSet;

import com.google.common.collect.ImmutableList;

public abstract class AbstractSetTest {

  protected static final String EXISTENT_SET_ID = "16287799-d37a-49fb-ac8c-09e9e9fcbd4d";
  protected static final String NONEXISTENT_SET_ID = "a3bd69dd-d50b-4aa6-accb-c1f9abaada55";

  protected static final String DUPLICATED_VALUE_USER_ERROR_MSG = "Field '%s' cannot have duplicated values. Value '%s' is already taken. Please, pass another value";
  protected static final String DUPLICATED_VALUE_DATABASE_ERROR_MSG = "duplicate key value violates unique constraint \"%s\"";
  protected static final String SET_SPEC_UNIQUE_CONSTRAINT = "set_spec_unique_constraint";
  protected static final String NAME_UNIQUE_CONSTRAINT = "name_unique_constraint";

  private static FilteringCondition MOCK_FILTERING_CONDITION = new FilteringCondition().withName("fk name")
    .withValue("fk value")
    .withSetSpec("fk setSpec");
  private static FilteringCondition MOCK_UPDATED_FILTERING_CONDITION = new FilteringCondition().withName("fk updated name")
    .withValue("fk updated value")
    .withSetSpec("fk updated setSpec");

  private static List<FilteringCondition> mockFilteringConditions = ImmutableList.of(MOCK_FILTERING_CONDITION);
  private static List<FilteringCondition> mockUpdatedFilteringConditions = ImmutableList.of(MOCK_UPDATED_FILTERING_CONDITION);

  protected static FolioSet INITIAL_TEST_SET_ENTRY = new FolioSet().withId(EXISTENT_SET_ID)
    .withName("test name")
    .withDescription("test description")
    .withSetSpec("test setSpec")
    .withFilteringConditions(mockFilteringConditions);

  protected static FolioSet UPDATE_SET_ENTRY = new FolioSet().withName("update name")
    .withDescription("update description")
    .withSetSpec("update SetSpec")
    .withFilteringConditions(mockUpdatedFilteringConditions);

  protected static FolioSet POST_SET_ENTRY = new FolioSet().withName("update name")
    .withDescription("update description")
    .withSetSpec("update SetSpec")
    .withFilteringConditions(mockFilteringConditions);

  protected void verifyMainSetData(FolioSet setWithExpectedData, FolioSet setToVerify, boolean checkIdEquals) {
    assertEquals(setWithExpectedData.getName(), setToVerify.getName());
    assertEquals(setWithExpectedData.getDescription(), setToVerify.getDescription());
    assertEquals(setWithExpectedData.getSetSpec(), setToVerify.getSetSpec());
    if (checkIdEquals) {
      assertEquals(EXISTENT_SET_ID, setToVerify.getId());
    } else {
      assertNotNull(setToVerify.getId());
    }
    verifyFilteringConditions(setWithExpectedData.getFilteringConditions(), setToVerify.getFilteringConditions());
  }

  private void verifyFilteringConditions(List<FilteringCondition> fkWithExpectedData, List<FilteringCondition> fkList) {
    assertFalse(fkList.isEmpty());
    FilteringCondition expectedFK = fkWithExpectedData.iterator()
      .next();
    FilteringCondition actualFK = fkList.iterator()
      .next();
    assertEquals(expectedFK.getName(), actualFK.getName());
    assertEquals(expectedFK.getValue(), actualFK.getValue());
    assertEquals(expectedFK.getSetSpec(), actualFK.getSetSpec());
  }

  protected void verifyMetadata(FolioSet folioSet) {
    assertEquals(OkapiMockServer.TEST_USER_ID, folioSet.getCreatedByUserId());
    assertEquals(OkapiMockServer.TEST_USER_ID, folioSet.getUpdatedByUserId());
    assertNotNull(folioSet.getCreatedDate());
    assertNotNull(folioSet.getUpdatedDate());
  }

}
