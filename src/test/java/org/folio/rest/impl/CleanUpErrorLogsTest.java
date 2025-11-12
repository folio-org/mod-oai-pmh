package org.folio.rest.impl;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.folio.rest.impl.OkapiMockServer.TEST_USER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.NotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.config.ApplicationConfig;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.common.TestUtil;
import org.folio.oaipmh.dao.ErrorsDao;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.service.ErrorsService;
import org.folio.oaipmh.service.InstancesService;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.ModuleName;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.s3.client.FolioS3Client;
import org.folio.spring.SpringContextUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

@ExtendWith(VertxExtension.class)
@ExtendWith(MockitoExtension.class)
@TestInstance(PER_CLASS)
class CleanUpErrorLogsTest {

  private final Logger logger = LogManager.getLogger(this.getClass());

  private static final int okapiPort = NetworkUtils.nextFreePort();
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final String CLEAN_UP_INSTANCES_PATH = "/oai-pmh/clean-up-error-logs";

  private final Header tenantHeader = new Header("X-Okapi-Tenant", OAI_TEST_TENANT);
  private final Header tenantHeaderInvalid = new Header("X-Okapi-Tenant", "invalid");
  private final Header okapiUrlHeader = new Header("X-Okapi-Url", "http://localhost:" + mockPort);
  private final Header okapiUserHeader = new Header("X-Okapi-User-Id", TEST_USER_ID);

  private static final GenericContainer<?> s3;
  private static final String MINIO_ENDPOINT;
  public static final String S3_ACCESS_KEY = "minio-access-key";
  public static final String S3_SECRET_KEY = "minio-secret-key";
  public static final int S3_PORT = 9000;
  public static final String BUCKET = "test-bucket";
  public static final String REGION = "us-west-2";

  @Autowired
  private FolioS3Client folioS3Client;

  @Autowired
  private InstancesService instancesService;

  @Autowired
  private InstancesDao instancesDao;

  @Autowired
  private ErrorsDao errorsDao;

  @Autowired
  private ErrorsService errorsService;

  @BeforeAll
  void setUpOnce(Vertx vertx, VertxTestContext testContext) throws Exception {
    logger.info("Test setup starting for {}.", ModuleName.getModuleName());
    System.setProperty("minio.bucket", BUCKET);
    System.setProperty("minio.region", REGION);
    System.setProperty("minio.accessKey", S3_ACCESS_KEY);
    System.setProperty("minio.secretKey", S3_SECRET_KEY);
    System.setProperty("minio.endpoint", MINIO_ENDPOINT);
    System.setProperty("clean.interval", "30");

    RestAssured.baseURI = "http://localhost:" + okapiPort;
    RestAssured.port = okapiPort;
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient.getInstance(vertx, OAI_TEST_TENANT).startPostgresTester();

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
            TestUtil.initializeTestContainerDbSchema(vertx, OAI_TEST_TENANT);
            new OkapiMockServer(vertx, mockPort).start(testContext);
          } catch (Exception e) {
            testContext.failNow(e);
          }
        }));
  }

  static {
    s3 = new GenericContainer<>("minio/minio:latest")
        .withEnv("MINIO_ACCESS_KEY", S3_ACCESS_KEY)
        .withEnv("MINIO_SECRET_KEY", S3_SECRET_KEY)
        .withCommand("server /data")
        .withExposedPorts(S3_PORT)
        .waitingFor(new HttpWaitStrategy().forPath("/minio/health/ready")
            .forPort(S3_PORT)
            .withStartupTimeout(Duration.ofSeconds(10))
      );
    s3.start();
    MINIO_ENDPOINT = format("http://%s:%s", s3.getHost(), s3.getFirstMappedPort());
  }

  @AfterAll
  void afterAll() {
    WebClientProvider.closeAll();
    PostgresClientFactory.closeAll();
  }

  @Test
  void shouldCallAllInnerMethods(VertxTestContext testContext) {
    var requestId = UUID.randomUUID().toString();
    var instanceId1 = UUID.randomUUID().toString();
    var errorMsg1 = "some error msg 1";
    var testTenantId = "oaiTest";

    testContext.verify(() -> {
      errorsService.log(testTenantId, requestId, instanceId1, errorMsg1);
      List<String> csvErrorLines = new ArrayList<>();
      csvErrorLines.add(new StringBuilder().append("Request ID").append(",")
          .append("Instance ID").append(",").append("Error message").toString());
      csvErrorLines.add(new StringBuilder().append(requestId).append(",")
          .append(instanceId1).append(",").append(errorMsg1).toString());
      RequestMetadataLb requestMetadata = new RequestMetadataLb();
      requestMetadata.setRequestId(UUID.fromString(requestId));
      requestMetadata.setLastUpdatedDate(OffsetDateTime.now());
      requestMetadata.setLinkToErrorFile("error-link");
      requestMetadata.setStartedDate(requestMetadata.getLastUpdatedDate().minusDays(365));

      instancesDao.saveRequestMetadata(requestMetadata, testTenantId)
          .onComplete(testContext.succeeding(requestMetadataLbSaved -> {
            errorsService.saveErrorsAndUpdateRequestMetadata(testTenantId, requestId)
                .onComplete(testContext.succeeding(requestMetadataUpdated -> {
                  instancesDao.getRequestMetadataCollection(0, 10, testTenantId)
                      .onComplete(testContext.succeeding(requestMetadataWithGeneratedLink -> {
                        errorsDao.getErrorsList(requestMetadata.getRequestId().toString(),
                              testTenantId)
                            .onComplete(testContext.succeeding(errorList -> {
                              assertEquals(1, errorList.size());

                              assertEquals(1, folioS3Client.list("").size());

                              RequestSpecification request = createBaseRequest(
                                  CLEAN_UP_INSTANCES_PATH, null, tenantHeader);
                              request.when()
                                  .post()
                                  .then()
                                  .statusCode(204);

                              errorsDao.getErrorsList(requestMetadata.getRequestId().toString(),
                                    testTenantId)
                                  .onComplete(testContext.succeeding(errorListNew -> {
                                    assertEquals(0, errorListNew.size());
                                  }));

                              assertEquals(0, folioS3Client.list("").size());

                              instancesService.getRequestMetadataByRequestId(requestId,
                                  testTenantId).onComplete(result -> {
                                    if (result.succeeded()) {
                                      assertEquals("", result.result().getPathToErrorFileInS3());
                                      assertEquals("", result.result().getLinkToErrorFile());
                                    } else {
                                      assertThrows(NotFoundException.class, () -> {
                                        logger.error("request metadata not found");
                                      });
                                    }
                                  });

                              instancesService.getRequestMetadataByRequestId(UUID.randomUUID()
                                  .toString(), testTenantId).onComplete(result -> {
                                    assertThrows(NotFoundException.class, () -> {
                                      logger.error("request metadata not found");
                                    });
                                  });

                              testContext.completeNow();
                            }));
                      }));
                }));
          }));
    });
  }

  @Test
  void shouldSucceedIfNoInstancesToSave(VertxTestContext testContext) {

    var testTenantId = "oaiTest";

    testContext.verify(() -> {

      instancesService.saveInstances(Collections.emptyList(), testTenantId)
            .onComplete(handler -> {
              assertTrue(handler.succeeded());
            });
      testContext.completeNow();
    });
  }

  @Test
  void shouldPassWhenNoErrorsFound(VertxTestContext testContext) {

    testContext.verify(() -> {
      RequestSpecification request = createBaseRequest(CLEAN_UP_INSTANCES_PATH, null,
          tenantHeader);
      request.when()
          .post()
          .then()
          .statusCode(204);
      testContext.completeNow();
    });
  }

  @Test
  void shouldThrowNotFoundExceptionWhenRequestIdNotFoundWhenCleanUpErrorLogs(
      VertxTestContext testContext) {

    testContext.verify(() -> {

      RequestSpecification request = createBaseRequest(CLEAN_UP_INSTANCES_PATH, null, tenantHeader);
      request.when()
          .post()
          .then()
          .statusCode(204);

      testContext.completeNow();
    });
  }

  @Test
  void shouldFailWhenInvalidTenant(VertxTestContext testContext) {

    testContext.verify(() -> {

      RequestSpecification request = createBaseRequest(CLEAN_UP_INSTANCES_PATH, null,
          tenantHeaderInvalid);
      request.when()
          .post()
          .then()
          .statusCode(500);

      testContext.completeNow();
    });
  }

  @Test
  void shouldThrowExceptionIfRequestIdNotFound(VertxTestContext testContext) {

    testContext.verify(() -> {


      var requestId = UUID.randomUUID().toString();

      RequestMetadataLb requestMetadata = new RequestMetadataLb();
      requestMetadata.setRequestId(UUID.fromString(requestId));
      requestMetadata.setLastUpdatedDate(OffsetDateTime.now());
      requestMetadata.setLinkToErrorFile("error-link");
      requestMetadata.setStartedDate(requestMetadata.getLastUpdatedDate().minusDays(365));

      var testTenantId = "oaiTest";
      instancesDao.saveRequestMetadata(requestMetadata, testTenantId)
          .onComplete(testContext.succeeding(requestMetadataLbSaved -> {

            RequestSpecification request = createBaseRequest(CLEAN_UP_INSTANCES_PATH, null,
                tenantHeader);
            request.when()
                .post()
                .then()
                .statusCode(204);

            instancesService.getRequestMetadataByRequestId(UUID.randomUUID().toString(),
                testTenantId).onComplete(result -> {
                  assertThrows(NotFoundException.class, () -> {
                    logger.error("request metadata not found");
                  });
                });

            testContext.completeNow();
          }));
    });
  }

  private RequestSpecification createBaseRequest(String path, ContentType contentType,
      Header tenantHeader) {
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
