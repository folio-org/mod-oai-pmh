package org.folio.rest.impl;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static org.folio.oaipmh.Constants.SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE;
import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.http.Header;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.config.ApplicationConfig;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.common.AbstractSetTest;
import org.folio.oaipmh.common.TestUtil;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.service.SetService;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.FilteringCondition;
import org.folio.rest.jaxrs.model.FolioSet;
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

@ExtendWith(VertxExtension.class)
@TestInstance(PER_CLASS)
class OaiPmhSetImplTest extends AbstractSetTest {

  private static final Logger logger = LogManager.getLogger(OaiPmhSetImplTest.class);

  private static final int okapiPort = NetworkUtils.nextFreePort();
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final String SET_PATH = "/oai-pmh/sets";
  private static final String FILTERING_CONDITIONS_PATH = "/oai-pmh/filtering-conditions";

  private Header tenantHeader = new Header("X-Okapi-Tenant", OAI_TEST_TENANT);
  private Header okapiUrlHeader = new Header("X-Okapi-Url", "http://localhost:" + mockPort);
  private Header okapiUserHeader = new Header("X-Okapi-User-Id", OkapiMockServer.TEST_USER_ID);

  private PostgresClientFactory postgresClientFactory;
  private SetService setService;

  @BeforeAll
  void setUpOnce(Vertx vertx, VertxTestContext testContext) throws Exception {
    logger.info("Test setup starting for {}.", ModuleName.getModuleName());
    PostgresClientFactory.setShouldResetPool(true);
    RestAssured.baseURI = "http://localhost:" + okapiPort;
    RestAssured.port = okapiPort;
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient client = PostgresClient.getInstance(vertx, OAI_TEST_TENANT);
    client.startPostgresTester();
    TestUtil.initializeTestContainerDbSchema(vertx, OAI_TEST_TENANT);

    JsonObject dpConfig = new JsonObject();
    dpConfig.put("http.port", okapiPort);
    DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(dpConfig);
    WebClientProvider.init(vertx);
    vertx.deployVerticle(RestVerticle.class.getName(), deploymentOptions,
        testContext.succeeding(v -> {
          try {
            Context context = vertx.getOrCreateContext();
            SpringContextUtil.init(vertx, context, ApplicationConfig.class);
            SpringContextUtil.autowireDependencies(this, context);
            new OkapiMockServer(vertx, mockPort).start(testContext);
            testContext.completeNow();
          } catch (Exception e) {
            testContext.failNow(e);
          }
        }));
  }

  @AfterAll
  void afterAll() {
    WebClientProvider.closeAll();
    PostgresClientFactory.closeAll();
  }

  @Test
  void shouldReturnSetItemWhenGetSetByIdAndItemWithSuchIdExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(getSetPathWithId(EXISTENT_SET_ID), null);
      String json = request.when()
          .get()
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .extract()
          .asString();
      FolioSet set = jsonStringToFolioSet(json);
      verifyMainSetData(INITIAL_TEST_SET_ENTRY, set, true);
      verifyMetadata(set);
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotReturnSetItemWhenGetSetByIdAndItemWithSuchIdDoesNotExist(
      VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(getSetPathWithId(NONEXISTENT_SET_ID), null);
      request.when()
          .get()
          .then()
          .statusCode(404)
          .contentType(ContentType.TEXT);
      testContext.completeNow();
    });
  }

  @Test
  void shouldUpdateSetItemWhenUpdateSetByIdAndItemWithSuchIdExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(getSetPathWithId(EXISTENT_SET_ID),
          ContentType.JSON).body(UPDATE_SET_ENTRY);
      request.when()
          .put()
          .then()
          .statusCode(204);

      String json = createBaseRequest(getSetPathWithId(EXISTENT_SET_ID), null).when()
          .get()
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .extract()
          .asString();
      FolioSet updatedSet = jsonStringToFolioSet(json);
      verifyMainSetData(UPDATE_SET_ENTRY, updatedSet, true);
      verifyMetadata(updatedSet);
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotUpdateSetItemWhenUpdateSetByIdAndItemWithSuchIdDoesNotExist(
      VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(getSetPathWithId(NONEXISTENT_SET_ID),
          ContentType.JSON).body(UPDATE_SET_ENTRY);
      request.when()
          .put()
          .then()
          .statusCode(404)
          .contentType(ContentType.TEXT);
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotUpdateSetItemWhenUpdateSetByIdWithEmptyName(VertxTestContext testContext) {
    testContext.verify(() -> {
      FolioSet folioSetWithEmptyName = new FolioSet().withId(EXISTENT_SET_ID)
          .withName("")
          .withDescription("description")
          .withSetSpec("setSpec");
      RequestSpecification request = createBaseRequest(getSetPathWithId(NONEXISTENT_SET_ID),
          ContentType.JSON).body(folioSetWithEmptyName);
      request.when()
          .put()
          .then()
          .statusCode(422)
          .contentType(ContentType.JSON)
          .body("errors[0].message", equalTo(format(SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE,
              "name")));
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotUpdateSetItemWhenUpdateSetByIdWithEmptySetSpec(VertxTestContext testContext) {
    testContext.verify(() -> {
      FolioSet folioSetWithEmptyName = new FolioSet().withId(EXISTENT_SET_ID)
          .withName("name")
          .withDescription("description")
          .withSetSpec("");
      RequestSpecification request = createBaseRequest(getSetPathWithId(NONEXISTENT_SET_ID),
          ContentType.JSON).body(folioSetWithEmptyName);
      request.when()
          .put()
          .then()
          .statusCode(422)
          .contentType(ContentType.JSON)
          .body("errors[0].message", equalTo(format(SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE,
              "setSpec")));
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotUpdateSetItemWhenUpdateSetByIdWithEmptyNameAndSetSpec(
      VertxTestContext testContext) {
    testContext.verify(() -> {
      FolioSet folioSetWithEmptyName = new FolioSet().withId(EXISTENT_SET_ID)
          .withName("")
          .withDescription("description")
          .withSetSpec("");
      RequestSpecification request = createBaseRequest(getSetPathWithId(EXISTENT_SET_ID),
          ContentType.JSON).body(folioSetWithEmptyName);
      request.when()
          .put()
          .then()
          .statusCode(422)
          .contentType(ContentType.JSON)
          .body("errors[0].message", equalTo(format(SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE,
              "name")))
          .body("errors[1].message", equalTo(format(SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE,
              "setSpec")));
      testContext.completeNow();
    });
  }

  @Test
  void shouldSaveSetItemWhenPostSet(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(SET_PATH, ContentType.JSON)
          .body(POST_SET_ENTRY);
      String json = request.when()
          .post()
          .then()
          .statusCode(201)
          .contentType(ContentType.JSON)
          .extract()
          .asString();
      FolioSet savedSet = jsonStringToFolioSet(json);
      verifyMainSetData(POST_SET_ENTRY, savedSet, false);
      verifyMetadata(savedSet);
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotSaveSetItemWhenPostSetWithIdAndItemWithSuchIdAlreadyExists(
      VertxTestContext testContext) {
    testContext.verify(() -> {
      POST_SET_ENTRY.setId(EXISTENT_SET_ID);
      RequestSpecification request = createBaseRequest(SET_PATH, ContentType.JSON)
          .body(POST_SET_ENTRY);
      request.when()
          .post()
          .then()
          .statusCode(400)
          .contentType(ContentType.TEXT);
      POST_SET_ENTRY.setId(null);
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotSaveSetItemWhenSaveSetWithEmptyName(VertxTestContext testContext) {
    testContext.verify(() -> {
      FolioSet folioSetWithEmptyName = new FolioSet().withId(UUID.randomUUID()
          .toString())
          .withName("")
          .withDescription("description")
          .withSetSpec("setSpec");

      RequestSpecification request = createBaseRequest(SET_PATH, ContentType.JSON)
          .body(folioSetWithEmptyName);
      request.when()
          .post()
          .then()
          .statusCode(422)
          .contentType(ContentType.JSON)
          .body("errors[0].message", equalTo(format(SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE,
              "name")));
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotSaveSetItemWhenSaveSetWithEmptyNameAndEmptySetSpec(VertxTestContext testContext) {
    testContext.verify(() -> {
      FolioSet folioSetWithEmptyName = new FolioSet().withId(UUID.randomUUID()
          .toString())
          .withName("")
          .withDescription("description")
          .withSetSpec("");

      RequestSpecification request = createBaseRequest(SET_PATH, ContentType.JSON)
          .body(folioSetWithEmptyName);
      request.when()
          .post()
          .then()
          .statusCode(422)
          .contentType(ContentType.JSON)
          .body("errors[0].message", equalTo(format(SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE,
              "name")))
          .body("errors[1].message", equalTo(format(SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE,
              "setSpec")));
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotSaveSetItemWhenSaveSetWithEmptySetSpec(VertxTestContext testContext) {
    testContext.verify(() -> {
      FolioSet folioSetWithEmptyName = new FolioSet().withId(UUID.randomUUID()
          .toString())
          .withName("name")
          .withDescription("description")
          .withSetSpec("");

      RequestSpecification request = createBaseRequest(SET_PATH, ContentType.JSON)
          .body(folioSetWithEmptyName);
      request.when()
          .post()
          .then()
          .statusCode(422)
          .contentType(ContentType.JSON)
          .body("errors[0].message", equalTo(format(SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE,
              "setSpec")));
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotSaveItemWhenSaveItemWithAlreadyExistedSetSpecValueCaseInsensitive(
      VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(SET_PATH, ContentType.JSON)
          .body(POST_SET_ENTRY);
      request.when()
          .post()
          .then()
          .statusCode(201)
          .contentType(ContentType.JSON);

      POST_SET_ENTRY.setName("unique value for name");

      request = createBaseRequest(SET_PATH, ContentType.JSON).body(POST_SET_ENTRY);
      request.when()
          .post()
          .then()
          .statusCode(422)
          .contentType(ContentType.JSON)
          .body("errors[0].message", equalTo(format(DUPLICATED_VALUE_USER_ERROR_MSG, "setSpec",
              POST_SET_ENTRY.getSetSpec().toLowerCase())));
      String oldNameValue = POST_SET_ENTRY.getName();
      POST_SET_ENTRY.setName(oldNameValue);
      String oldSetSpecValue = POST_SET_ENTRY.getSetSpec();
      POST_SET_ENTRY.setSetSpec(oldSetSpecValue);
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotSaveItemWhenSaveItemWithAlreadyExistedNameValueCaseInsensitive(
      VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request =
          createBaseRequest(SET_PATH, ContentType.JSON).body(POST_SET_ENTRY);
      request.when()
          .post()
          .then()
          .statusCode(201)
          .contentType(ContentType.JSON);

      POST_SET_ENTRY.setSetSpec("unique value for setSpec");

      request = createBaseRequest(SET_PATH, ContentType.JSON).body(POST_SET_ENTRY);
      request.when()
          .post()
          .then()
          .statusCode(422)
          .contentType(ContentType.JSON)
          .body("errors[0].message", equalTo(format(DUPLICATED_VALUE_USER_ERROR_MSG,
              "name", POST_SET_ENTRY.getName().toLowerCase())));
      String oldNameValue = POST_SET_ENTRY.getName();
      POST_SET_ENTRY.setName(oldNameValue);
      String oldSetSpecValue = POST_SET_ENTRY.getSetSpec();
      POST_SET_ENTRY.setSetSpec(oldSetSpecValue);
      testContext.completeNow();
    });
  }

  @Test
  void shouldDeleteSetItemWhenDeleteSetByIdAndItemWithSuchIdExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(getSetPathWithId(EXISTENT_SET_ID), null);
      request.when()
          .delete()
          .then()
          .statusCode(204);
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotDeleteSetItemWhenDeleteSetByIdAndItemWithSuchIdDoesNotExist(
      VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(getSetPathWithId(NONEXISTENT_SET_ID), null);
      request.when()
          .delete()
          .then()
          .statusCode(404)
          .contentType(ContentType.TEXT);
      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnSetItemList(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(SET_PATH, null);
      request.when()
          .get()
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .body("$", hasKey("totalRecords"));
      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnFilteringConditions(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(FILTERING_CONDITIONS_PATH, null);
      request.when()
          .get()
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .body("setsFilteringConditions.size()", is(5));
      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnInternalServerErrorWhenExceptionOccursInGetSetById(VertxTestContext testContext) {
    testContext.verify(() -> {
      // Simulate an exception by passing an invalid ID (e.g., null or malformed)
      RequestSpecification request = createBaseRequest(getSetPathWithId("invalid-id-for-exception"), null);
      request.when()
          .get()
          .then()
          .statusCode(500)
          .contentType(ContentType.TEXT);
      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnInternalServerErrorWhenExceptionOccursInPutSetById(VertxTestContext testContext) {
    testContext.verify(() -> {
      // Simulate an exception by passing an invalid ID (e.g., null or malformed)
      FolioSet folioSet = new FolioSet().withName("name").withSetSpec("spec");
      RequestSpecification request = createBaseRequest(getSetPathWithId("invalid-id-for-exception"), ContentType.JSON)
          .body(folioSet);
      request.when()
          .put()
          .then()
          .statusCode(500)
          .contentType(ContentType.TEXT);
      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnInternalServerErrorWhenExceptionOccursInGetOaiPmhSets(VertxTestContext testContext) {
    testContext.verify(() -> {
      // Simulate an exception by passing invalid query parameters
      RequestSpecification request = createBaseRequest(SET_PATH, null)
          .param("offset", "-1")
          .param("limit", "-1");
      request.when()
          .get()
          .then()
          .statusCode(500)
          .contentType(ContentType.TEXT);
      testContext.completeNow();
    });
  }

  private RequestSpecification createBaseRequest(String path, ContentType contentType) {
    RequestSpecification requestSpecification = RestAssured.given()
        .header(okapiUrlHeader)
        .header(tenantHeader)
        .header(okapiUserHeader)
        .basePath(path);
    if (nonNull(contentType)) {
      requestSpecification.contentType(contentType);
    }
    return requestSpecification;
  }

  private FolioSet jsonStringToFolioSet(String json) {
    JsonObject jsonObject = new JsonObject(json);
    List<FilteringCondition> fkList = jsonObject.getJsonArray("filteringConditions")
        .stream()
        .map(JsonObject.class::cast)
        .map(this::jsonObjectToFilteringCondition)
        .collect(Collectors.toList());
    return new FolioSet().withId(jsonObject.getString("id"))
        .withName(jsonObject.getString("name"))
        .withDescription(jsonObject.getString("description"))
        .withSetSpec(jsonObject.getString("setSpec"))
        .withFilteringConditions(fkList)
        .withCreatedByUserId(jsonObject.getString("createdByUserId"))
        .withCreatedDate(getDate(jsonObject, "createdDate"))
        .withUpdatedByUserId(jsonObject.getString("updatedByUserId"))
        .withUpdatedDate(getDate(jsonObject, "updatedDate"));
  }

  private FilteringCondition jsonObjectToFilteringCondition(JsonObject jsonObject) {
    return new FilteringCondition().withName(jsonObject.getString("name"))
      .withValue(jsonObject.getString("value"))
      .withSetSpec(jsonObject.getString("setSpec"));
  }

  private Date getDate(JsonObject jsonObject, String field) {
    String strDate = jsonObject.getValue(field)
        .toString()
        .split("\\.")[0];
    LocalDateTime localDateTime = LocalDateTime.parse(strDate,
        DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    return Date.from(localDateTime.toInstant(ZoneOffset.UTC));
  }

  private String getSetPathWithId(String id) {
    return SET_PATH + "/" + id;
  }

  @Override
  public PostgresClientFactory getPostgresClientFactory() {
    return postgresClientFactory;
  }

  @Autowired
  public void setPostgresClientFactory(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public SetService getSetService() {
    return setService;
  }

  @Autowired
  public void setSetService(SetService setService) {
    this.setService = setService;
  }
}
