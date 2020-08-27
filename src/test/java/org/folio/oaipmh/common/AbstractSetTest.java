package org.folio.oaipmh.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.folio.rest.jaxrs.model.FilteringCondition;
import org.folio.rest.jaxrs.model.FolioSet;

import com.google.common.collect.ImmutableList;

public abstract class AbstractSetTest {

  protected static final String TEST_TENANT_ID = "oaiTest";
  protected static final String TEST_USER_ID = "30fde4be-2d1a-4546-8d6c-b468caca2720";
  protected static final String EXISTENT_SET_ID = "16287799-d37a-49fb-ac8c-09e9e9fcbd4d";
  protected static final String NONEXISTENT_SET_ID = "a3bd69dd-d50b-4aa6-accb-c1f9abaada55";

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
    assertEquals(folioSet.getCreatedByUserId(), TEST_USER_ID);
    assertEquals(folioSet.getUpdatedByUserId(), TEST_USER_ID);
    assertNotNull(folioSet.getCreatedDate());
    assertNotNull(folioSet.getUpdatedDate());
  }

}
