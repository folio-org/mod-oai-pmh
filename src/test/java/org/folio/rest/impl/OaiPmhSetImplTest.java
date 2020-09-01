package org.folio.rest.impl;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.folio.config.ApplicationConfig;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.liquibase.SingleConnectionProvider;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.dao.SetDao;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.FolioSet;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.spring.SpringContextUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.http.Header;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
@TestInstance(PER_CLASS)
class OaiPmhSetImplTest {
  private static final Logger logger = LoggerFactory.getLogger(OaiPmhSetImplTest.class);

  private static final String TEST_USER_ID = UUID.randomUUID()
    .toString();

  private static final int okapiPort = NetworkUtils.nextFreePort();
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final String SET_PATH = "/oai-pmh/sets";
  private static final String FILTERING_CONDITIONS_PATH = "/oai-pmh/filtering-conditions";
  private static final String EXISTENT_SET_ID = "16287799-d37a-49fb-ac8c-09e9e9fcbd4d";
  private static final String NONEXISTENT_SET_ID = "a3bd69dd-d50b-4aa6-accb-c1f9abaada55";

  private static final String DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_ERROR_MSG = "duplicate key value violates unique constraint \"%s\"";
  private static final String SET_SPEC_UNIQUE_CONSTRAINT = "set_spec_unique_constraint";
  private static final String NAME_UNIQUE_CONSTRAINT = "name_unique_constraint";

  private FolioSet INITIAL_TEST_SET_ENTRY = new FolioSet().withId(EXISTENT_SET_ID)
    .withName("test name")
    .withDescription("test description")
    .withSetSpec("test setSpec");

  private FolioSet UPDATE_SET_ENTRY = new FolioSet().withName("update name")
    .withDescription("update description")
    .withSetSpec("update SetSpec");

  private FolioSet POST_SET_ENTRY = new FolioSet().withName("post name")
    .withDescription("post description")
    .withSetSpec("post SetSpec");

  private Header tenantHeader = new Header("X-Okapi-Tenant", OAI_TEST_TENANT);
  private Header okapiUrlHeader = new Header("X-Okapi-Url", "http://localhost:" + mockPort);
  private Header okapiUserHeader = new Header("X-Okapi-User-Id", TEST_USER_ID);

  private SetDao setDao;

  @BeforeAll
  void setUpOnce(Vertx vertx, VertxTestContext testContext) throws Exception {
    String moduleName = PomReader.INSTANCE.getModuleName()
      .replaceAll("_", "-");
    String moduleVersion = PomReader.INSTANCE.getVersion();
    String moduleId = moduleName + "-" + moduleVersion;
    logger.info("Test setup starting for " + moduleId);

    RestAssured.baseURI = "http://localhost:" + okapiPort;
    RestAssured.port = okapiPort;
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

    PostgresClient client = PostgresClient.getInstance(vertx);
    client.startEmbeddedPostgres();

    JsonObject dpConfig = new JsonObject();
    dpConfig.put("http.port", okapiPort);
    DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(dpConfig);

    vertx.deployVerticle(RestVerticle.class.getName(), deploymentOptions, testContext.succeeding(v -> {
      try {
        Context context = vertx.getOrCreateContext();
        SpringContextUtil.init(vertx, context, ApplicationConfig.class);
        SpringContextUtil.autowireDependencies(this, context);
        try (Connection connection = SingleConnectionProvider.getConnection(vertx, OAI_TEST_TENANT)) {
            connection.prepareStatement("create schema oaitest_mod_oai_pmh")
              .execute();
        } catch (Exception ex) {
          testContext.failNow(ex);
        }
        LiquibaseUtil.initializeSchemaForTenant(vertx, OAI_TEST_TENANT);
        new OkapiMockServer(vertx, mockPort).start(testContext);
        testContext.completeNow();
      } catch (Exception e) {
        testContext.failNow(e);
      }
    }));
  }

  @BeforeEach
  private void initTestData(VertxTestContext testContext) {
    setDao.saveSet(INITIAL_TEST_SET_ENTRY, OAI_TEST_TENANT, TEST_USER_ID)
      .onComplete(result -> {
        if (result.failed()) {
          testContext.failNow(result.cause());
        } else {
          testContext.completeNow();
        }
      });
  }

  @AfterEach
  private void cleanTestData(VertxTestContext testContext) {
    setDao.getSetList(0, 100, OAI_TEST_TENANT).onSuccess(folioSetCollection -> {
      List<Future> list = new ArrayList<>();
      folioSetCollection.getSets().forEach(set -> {
        list.add(setDao.deleteSetById(set.getId(), OAI_TEST_TENANT));
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

  @AfterAll
  void afterAll(Vertx vertx, VertxTestContext testContext) {
    PostgresClientFactory.closeAll();
    vertx.close(testContext.succeeding(res -> {
      PostgresClient.stopEmbeddedPostgres();
      testContext.completeNow();
    }));
  }

  @Test
  void shouldReturnSetItem_whenGetSetByIdAndItemWithSuchIdExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(getSetPathWithId(EXISTENT_SET_ID), null);
      request.when()
        .get()
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON);
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotReturnSetItem_whenGetSetByIdAndItemWithSuchIdDoesNotExist(VertxTestContext testContext) {
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
  void shouldUpdateSetItem_whenUpdateSetByIdAndItemWithSuchIdExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(getSetPathWithId(EXISTENT_SET_ID), ContentType.JSON).body(UPDATE_SET_ENTRY);
      request.when()
        .put()
        .then()
        .statusCode(204);
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotUpdateSetItem_whenUpdateSetByIdAndItemWithSuchIdDoesNotExist(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(getSetPathWithId(NONEXISTENT_SET_ID), ContentType.JSON).body(UPDATE_SET_ENTRY);
      request.when()
        .put()
        .then()
        .statusCode(404)
        .contentType(ContentType.TEXT);
      testContext.completeNow();
    });
  }

  @Test
  void shouldSaveSetItem_whenPostSet(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(SET_PATH, ContentType.JSON).body(POST_SET_ENTRY);
      request.when()
        .post()
        .then()
        .statusCode(201)
        .contentType(ContentType.JSON);
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotSaveSetItem_whenPostSetWithIdAndItemWithSuchIdAlreadyExists(VertxTestContext testContext) {
    testContext.verify(() -> {
      POST_SET_ENTRY.setId(EXISTENT_SET_ID);
      RequestSpecification request = createBaseRequest(SET_PATH, ContentType.JSON).body(POST_SET_ENTRY);
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
  void shouldNotSaveItem_whenSaveItemWithAlreadyExistedSetSpecValue(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(SET_PATH, ContentType.JSON).body(POST_SET_ENTRY);
      request.when()
        .post()
        .then()
        .statusCode(201)
        .contentType(ContentType.JSON);

      String oldValue = POST_SET_ENTRY.getName();
      POST_SET_ENTRY.setName("unique value for name");

      request = createBaseRequest(SET_PATH, ContentType.JSON).body(POST_SET_ENTRY);
      request.when()
        .post()
        .then()
        .statusCode(422)
        .contentType(ContentType.JSON)
        .body("errors[0].message", equalTo(format(DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_ERROR_MSG, SET_SPEC_UNIQUE_CONSTRAINT)));
      POST_SET_ENTRY.setName(oldValue);
      testContext.completeNow();
    });
  }

  @Test
  void shouldNotSaveItem_whenSaveItemWithAlreadyExistedNameValue(VertxTestContext testContext) {
    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(SET_PATH, ContentType.JSON).body(POST_SET_ENTRY);
      request.when()
        .post()
        .then()
        .statusCode(201)
        .contentType(ContentType.JSON);

      String oldValue = POST_SET_ENTRY.getSetSpec();
      POST_SET_ENTRY.setSetSpec("unique value for setSpec");

      request = createBaseRequest(SET_PATH, ContentType.JSON).body(POST_SET_ENTRY);
      request.when()
        .post()
        .then()
        .statusCode(422)
        .contentType(ContentType.JSON)
        .body("errors[0].message", equalTo(format(DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_ERROR_MSG, NAME_UNIQUE_CONSTRAINT)));
      POST_SET_ENTRY.setSetSpec(oldValue);
      testContext.completeNow();
    });
  }

  @Test
  void shouldDeleteSetItem_whenDeleteSetByIdAndItemWithSuchIdExists(VertxTestContext testContext) {
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
  void shouldNotDeleteSetItem_whenDeleteSetByIdAndItemWithSuchIdDoesNotExist(VertxTestContext testContext) {
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

  private String getSetPathWithId(String id) {
    return SET_PATH + "/" + id;
  }

  @Autowired
  public void setSetDao(SetDao setDao) {
    this.setDao = setDao;
  }
}
