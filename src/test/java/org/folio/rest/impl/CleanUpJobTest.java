package org.folio.rest.impl;

import static java.util.Objects.nonNull;
import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.folio.rest.impl.OkapiMockServer.TEST_USER_ID;
import static org.folio.rest.jooq.Tables.SET_LB;
import static org.folio.rest.jooq.tables.RequestMetadataLb.REQUEST_METADATA_LB;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import java.sql.Connection;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.NotFoundException;

import org.folio.config.ApplicationConfig;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.liquibase.SingleConnectionProvider;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.RestVerticle;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.folio.rest.jooq.tables.records.RequestMetadataLbRecord;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.spring.SpringContextUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

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
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
@TestInstance(PER_CLASS)
public class CleanUpJobTest {

  private static final int okapiPort = NetworkUtils.nextFreePort();
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final String CLEAN_UP_INSTANCES_PATH = "/oai/clean-up-instances";
  private PostgresClientFactory postgresClientFactory;

  private static final int INSTANCES_EXPIRATION_TIME_IN_SECONDS = 7300;

  private Header tenantHeader = new Header("X-Okapi-Tenant", OAI_TEST_TENANT);
  private Header okapiUrlHeader = new Header("X-Okapi-Url", "http://localhost:" + mockPort);
  private Header okapiUserHeader = new Header("X-Okapi-User-Id", TEST_USER_ID);

  private static final String EXPIRED_REQUEST_ID_1 = "";
  private static final String EXPIRED_REQUEST_ID_2 = "";
  private static final String NOT_EXPIRED_REQUEST_ID_3 = "";
  private static final String NOT_EXPIRED_REQUEST_ID_4 = "";

  private List<String> requestIds = List.of(EXPIRED_REQUEST_ID_1, EXPIRED_REQUEST_ID_2, NOT_EXPIRED_REQUEST_ID_3,
      NOT_EXPIRED_REQUEST_ID_4);

  @BeforeAll
  void setUpOnce(Vertx vertx, VertxTestContext testContext) throws Exception {
    String moduleName = PomReader.INSTANCE.getModuleName()
      .replaceAll("_", "-");
    String moduleVersion = PomReader.INSTANCE.getVersion();
    String moduleId = moduleName + "-" + moduleVersion;

    RestAssured.baseURI = "http://localhost:" + okapiPort;
    RestAssured.port = okapiPort;
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

    postgresClientFactory = new PostgresClientFactory(vertx);
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
  private void initSampleData(VertxTestContext testContext) {
    OffsetDateTime firstNotExpiredDate = new Date().toInstant()
      .atOffset(ZoneOffset.of(ZoneId.systemDefault()
        .getId()));
    OffsetDateTime secondNotExpiredDate = new Date().toInstant()
      .atOffset(ZoneOffset.of(ZoneId.systemDefault()
        .getId()));
    OffsetDateTime firstExpiredDate = new Date().toInstant()
      .atOffset(ZoneOffset.of(ZoneId.systemDefault()
        .getId()))
      .minusSeconds(INSTANCES_EXPIRATION_TIME_IN_SECONDS);
    OffsetDateTime secondExpiredDate = new Date().toInstant()
      .atOffset(ZoneOffset.of(ZoneId.systemDefault()
        .getId()))
      .minusSeconds(INSTANCES_EXPIRATION_TIME_IN_SECONDS);

    RequestMetadataLb firstExpiredRequestMetadata = new RequestMetadataLb().setId(UUID.fromString(EXPIRED_REQUEST_ID_1))
      .setRequestId(EXPIRED_REQUEST_ID_1)
      .setLastUpdatedDate(firstExpiredDate);
    RequestMetadataLb secondExpiredRequestMetadata = new RequestMetadataLb().setId(UUID.fromString(EXPIRED_REQUEST_ID_2))
      .setRequestId(EXPIRED_REQUEST_ID_2)
      .setLastUpdatedDate(secondExpiredDate);
    RequestMetadataLb firstNotExpiredRequestMetadata = new RequestMetadataLb().setId(UUID.fromString(NOT_EXPIRED_REQUEST_ID_3))
      .setRequestId(NOT_EXPIRED_REQUEST_ID_3)
      .setLastUpdatedDate(firstNotExpiredDate);
    RequestMetadataLb secondNotExpiredRequestMetadata = new RequestMetadataLb().setId(UUID.fromString(NOT_EXPIRED_REQUEST_ID_3))
      .setRequestId(NOT_EXPIRED_REQUEST_ID_3)
      .setLastUpdatedDate(secondNotExpiredDate);

    List<Future> futures = new ArrayList<>();
    List
      .of(firstExpiredRequestMetadata, secondExpiredRequestMetadata, firstNotExpiredRequestMetadata,
          secondNotExpiredRequestMetadata)
      .forEach(elem ->
        futures.add(saveRequestMetadata(elem)));

    CompositeFuture.all(futures)
      .onSuccess(reply -> testContext.completeNow())
      .onFailure(testContext::failNow);
  }

  @Test
  void shouldReturn204AndClearExpiredInstances(VertxTestContext testContext) {
    RequestSpecification request = createBaseRequest(CLEAN_UP_INSTANCES_PATH, null);
    request.when()
      .post()
      .then()
      .statusCode(204);
  }

  //will add batch save
  private Future<RequestMetadataLb> saveRequestMetadata(RequestMetadataLb requestMetadata) {
    return postgresClientFactory.getQueryExecutor(OAI_TEST_TENANT)
      .transaction(queryExecutor -> queryExecutor.executeAny(dslContext -> dslContext.insertInto(REQUEST_METADATA_LB)
        .set(toDbRecord(requestMetadata)))
        .map(row -> requestMetadata));
  }

  private RequestMetadataLbRecord toDbRecord(RequestMetadataLb pojo) {
    RequestMetadataLbRecord record = new RequestMetadataLbRecord();
    record.setId(pojo.getId());
    record.setRequestId(pojo.getRequestId());
    record.setLastUpdatedDate(pojo.getLastUpdatedDate());
    return record;
  }

  private Future<Boolean> deleteRequestMetadataById(String requestId) {
    return postgresClientFactory.getQueryExecutor(OAI_TEST_TENANT)
      .transaction(queryExecutor -> queryExecutor.execute(dslContext -> dslContext.deleteFrom(REQUEST_METADATA_LB)
        .where(REQUEST_METADATA_LB.ID.eq(UUID.fromString(requestId))))
        .map(res -> {
          if (res == 1) {
            return true;
          }
          throw new NotFoundException(String.format("Request metadata entity with id:%s doesn't exist", requestId));
        }));
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

}
