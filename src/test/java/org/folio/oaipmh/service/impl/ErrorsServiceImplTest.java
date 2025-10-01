package org.folio.oaipmh.service.impl;

import static java.lang.String.format;
import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.InputStream;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.config.ApplicationConfig;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.common.AbstractErrorsTest;
import org.folio.oaipmh.common.TestUtil;
import org.folio.oaipmh.dao.ErrorsDao;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.service.ErrorsService;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.impl.OkapiMockServer;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
public class ErrorsServiceImplTest extends AbstractErrorsTest {

  private final Logger logger = LogManager.getLogger(this.getClass());

  private static final String TEST_TENANT_ID = "oaiTest";
  private static final int mockPort = NetworkUtils.nextFreePort();
  private static final GenericContainer<?> s3;
  private static final String MINIO_ENDPOINT;
  public static final String S3_ACCESS_KEY = "minio-access-key";
  public static final String S3_SECRET_KEY = "minio-secret-key";
  public static final int S3_PORT = 9000;
  public static final String BUCKET = "test-bucket";
  public static final String REGION = "us-west-2";

  @Autowired
  private FolioS3Client folioS3Client;

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

  @Autowired
  private ErrorsDao errorsDao;

  @Autowired
  private ErrorsService errorsService;

  @Autowired
  private InstancesDao instancesDao;

  @BeforeAll
  void setUpClass(Vertx vertx, VertxTestContext testContext) throws Exception {
    System.setProperty("minio.bucket", BUCKET);
    System.setProperty("minio.region", REGION);
    System.setProperty("minio.accessKey", S3_ACCESS_KEY);
    System.setProperty("minio.secretKey", S3_SECRET_KEY);
    System.setProperty("minio.endpoint", MINIO_ENDPOINT);
    PostgresClientFactory.setShouldResetPool(true);
    logger.info("Test setup starting for {}.", ModuleName.getModuleName());
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient.getInstance(vertx, OAI_TEST_TENANT).startPostgresTester();
    Context context = vertx.getOrCreateContext();
    SpringContextUtil.init(vertx, context, ApplicationConfig.class);
    SpringContextUtil.autowireDependencies(this, context);
    new OkapiMockServer(vertx, mockPort).start(testContext);
    WebClientProvider.init(vertx);
    TestUtil.initializeTestContainerDbSchema(vertx, OAI_TEST_TENANT);
    testContext.completeNow();
  }

  @AfterAll
  static void tearDownClass(Vertx vertx, VertxTestContext testContext) {
    PostgresClientFactory.closeAll();
    WebClientProvider.closeAll();
    vertx.close(testContext.succeeding(res -> {
      PostgresClient.stopPostgresTester();
      testContext.completeNow();
    }));
  }

  @Test
  void shouldSaveErrorsWithLinkToErrorFile(VertxTestContext testContext) {
    var requestId = UUID.randomUUID().toString();
    var instanceId1 = UUID.randomUUID().toString();
    var instanceId2 = UUID.randomUUID().toString();
    var instanceId3 = UUID.randomUUID().toString();
    var errorMsg1 = "some error msg 1";
    var errorMsg2 = "some error msg 2";
    var errorMsg3 = "some error msg 3";
    testContext.verify(() -> {
      errorsService.log(TEST_TENANT_ID, requestId, instanceId1, errorMsg1);
      errorsService.log(TEST_TENANT_ID, requestId, instanceId2, errorMsg2);
      errorsService.log(TEST_TENANT_ID, requestId, instanceId3, errorMsg3);
      List<String> csvErrorLines = new ArrayList<>();
      csvErrorLines.add(new StringBuilder().append("Request ID").append(",")
          .append("Instance ID").append(",").append("Error message").toString());
      csvErrorLines.add(new StringBuilder().append(requestId).append(",")
          .append(instanceId1).append(",").append(errorMsg1).toString());
      csvErrorLines.add(new StringBuilder().append(requestId).append(",")
          .append(instanceId2).append(",").append(errorMsg2).toString());
      csvErrorLines.add(new StringBuilder().append(requestId).append(",")
          .append(instanceId3).append(",").append(errorMsg3).toString());
      RequestMetadataLb requestMetadata = new RequestMetadataLb();
      requestMetadata.setRequestId(UUID.fromString(requestId));
      requestMetadata.setLastUpdatedDate(OffsetDateTime.now());
      requestMetadata.setStartedDate(requestMetadata.getLastUpdatedDate());
      instancesDao.saveRequestMetadata(requestMetadata, TEST_TENANT_ID)
          .onComplete(testContext.succeeding(requestMetadataLbSaved -> {
            errorsService.saveErrorsAndUpdateRequestMetadata(TEST_TENANT_ID, requestId)
                .onComplete(testContext.succeeding(requestMetadataUpdated -> {
                  assertNotNull(requestMetadataUpdated);
                  var linkToErrorFile = requestMetadataUpdated.getLinkToErrorFile();
                  // At the moment, only path to error file has been saved.
                  assertNull(linkToErrorFile);
                  instancesDao.getRequestMetadataCollection(0, 10, TEST_TENANT_ID)
                      .onComplete(testContext.succeeding(requestMetadataWithGeneratedLink -> {
                        var generatedLinkToErrorFile = requestMetadataWithGeneratedLink
                            .getRequestMetadataCollection().get(0).getLinkToErrorFile();
                        assertNotNull(generatedLinkToErrorFile);
                        verifyErrorCsvFile(generatedLinkToErrorFile, csvErrorLines);
                        errorsDao.getErrorsList(requestMetadata.getRequestId().toString(),
                              TEST_TENANT_ID)
                            .onComplete(testContext.succeeding(errorList -> {
                              assertEquals(3, errorList.size());
                              errorList.forEach(err -> assertTrue(err.getErrorMsg()
                                  .startsWith("some error msg")));
                              testContext.completeNow();
                            }));
                      }));
                }));
          }));
    });
  }

  @Test
  void shouldUpdateRequestMetadataAndSaveErrorWhenErrorFound(VertxTestContext testContext) {
    var requestId = UUID.randomUUID().toString();
    var instanceId = UUID.randomUUID().toString();
    var errorMsg = "some error msg";
    testContext.verify(() -> {
      errorsService.log(TEST_TENANT_ID, requestId, instanceId, errorMsg);
      List<String> csvErrorLines = new ArrayList<>();
      csvErrorLines.add(new StringBuilder().append("Request ID").append(",")
          .append("Instance ID").append(",").append("Error message").toString());
      csvErrorLines.add(new StringBuilder().append(requestId).append(",")
          .append(instanceId).append(",").append(errorMsg).toString());
      RequestMetadataLb requestMetadata = new RequestMetadataLb();
      requestMetadata.setRequestId(UUID.fromString(requestId));
      requestMetadata.setLastUpdatedDate(OffsetDateTime.now());
      requestMetadata.setStartedDate(requestMetadata.getLastUpdatedDate());
      instancesDao.saveRequestMetadata(requestMetadata, TEST_TENANT_ID)
          .onComplete(testContext.succeeding(requestMetadataLbSaved -> {
            errorsService.saveErrorsAndUpdateRequestMetadata(TEST_TENANT_ID,
                  requestMetadataLbSaved.getRequestId().toString())
                .onComplete(testContext.succeeding(requestMetadataUpdated -> {
                  assertNotNull(requestMetadataUpdated);
                  var linkToErrorFile = requestMetadataUpdated.getLinkToErrorFile();
                  // At the moment, only path to error file has been saved.
                  assertNull(linkToErrorFile);
                  instancesDao.getRequestMetadataCollection(0, 10, TEST_TENANT_ID)
                      .onComplete(testContext.succeeding(requestMetadataWithGeneratedLink -> {
                        var generatedLinkToErrorFile = requestMetadataWithGeneratedLink
                            .getRequestMetadataCollection().get(0).getLinkToErrorFile();
                        assertNotNull(generatedLinkToErrorFile);
                        verifyErrorCsvFile(generatedLinkToErrorFile, csvErrorLines);
                        errorsDao.getErrorsList(requestMetadata.getRequestId().toString(),
                            TEST_TENANT_ID)
                            .onComplete(testContext.succeeding(errorList -> {
                              assertEquals(1, errorList.size());
                              assertEquals(errorMsg, errorList.get(0).getErrorMsg());
                              testContext.completeNow();
                            }));
                      }));
                }));
          }));
    });
  }

  @Test
  void shouldOnlyUpdateRequestMetadataAndNotSaveErrorsWhenErrorsNotFound(
      VertxTestContext testContext) {
    var requestId = UUID.randomUUID().toString();
    testContext.verify(() -> {
      RequestMetadataLb requestMetadata = new RequestMetadataLb();
      requestMetadata.setRequestId(UUID.fromString(requestId));
      requestMetadata.setLastUpdatedDate(OffsetDateTime.now());
      requestMetadata.setStartedDate(requestMetadata.getLastUpdatedDate());
      instancesDao.saveRequestMetadata(requestMetadata, TEST_TENANT_ID)
          .onComplete(testContext.succeeding(requestMetadataLbSaved -> {
            errorsService.saveErrorsAndUpdateRequestMetadata(TEST_TENANT_ID,
                  requestMetadataLbSaved.getRequestId().toString())
                .onComplete(testContext.succeeding(requestMetadataUpdated -> {
                  assertNotNull(requestMetadataUpdated);
                  assertNull(requestMetadataUpdated.getLinkToErrorFile());
                  errorsDao.getErrorsList(requestMetadata.getRequestId().toString(),
                      TEST_TENANT_ID)
                      .onComplete(testContext.succeeding(errorList -> {
                        assertTrue(errorList.isEmpty());
                        testContext.completeNow();
                      }));
                }));
          }));
    });
  }

  @Test
  void shouldDeleteErrorsByRequestIdWhenErrorFound(VertxTestContext testContext) {
    var requestId = UUID.randomUUID().toString();
    var instanceId = UUID.randomUUID().toString();
    var errorMsg = "some error msg";
    testContext.verify(() -> {
      errorsService.log(TEST_TENANT_ID, requestId, instanceId, errorMsg);
      List<String> csvErrorLines = new ArrayList<>();
      csvErrorLines.add(new StringBuilder().append(requestId).append(",")
          .append(instanceId).append(",").append(errorMsg).toString());
      RequestMetadataLb requestMetadata = new RequestMetadataLb();
      requestMetadata.setRequestId(UUID.fromString(requestId));
      requestMetadata.setLastUpdatedDate(OffsetDateTime.now());
      requestMetadata.setStartedDate(requestMetadata.getLastUpdatedDate());
      instancesDao.saveRequestMetadata(requestMetadata, TEST_TENANT_ID)
          .onComplete(testContext.succeeding(requestMetadataLbSaved -> {
            errorsService.saveErrorsAndUpdateRequestMetadata(TEST_TENANT_ID,
                  requestMetadataLbSaved.getRequestId().toString())
                .onComplete(testContext.succeeding(requestMetadataUpdated -> {
                  errorsDao.getErrorsList(requestMetadata.getRequestId().toString(),
                      TEST_TENANT_ID)
                      .onComplete(testContext.succeeding(errorList -> {
                        assertEquals(1, errorList.size());
                        errorsService.deleteErrorsByRequestId(TEST_TENANT_ID, requestId)
                            .onComplete(testContext.succeeding(errorDeleted -> {
                              assertTrue(errorDeleted);
                              errorsDao.getErrorsList(requestId, TEST_TENANT_ID)
                                  .onComplete(testContext.succeeding(errorListAfterDeleted -> {
                                    assertEquals(0, errorListAfterDeleted.size());
                                    testContext.completeNow();
                                  }));
                            }));
                      }));
                }));
          }));
    });
  }

  private void verifyErrorCsvFile(String linkToError, List<String> initErrorFileContent) {
    try (InputStream inputStream = folioS3Client.read(linkToError);
        Scanner scanner = new Scanner(inputStream)) {
      List<String> listCsvLines = new ArrayList<>();
      while (scanner.hasNextLine()) {
        var csvLine = scanner.nextLine();
        listCsvLines.add(csvLine);
      }
      Collections.sort(listCsvLines);
      Collections.sort(initErrorFileContent);
      assertIterableEquals(initErrorFileContent, listCsvLines);
    } catch (Exception e) {
      fail(e);
    }
  }

  @Override
  protected ErrorsDao getErrorsDao() {
    return errorsDao;
  }

  @Override
  protected InstancesDao getInstancesDao() {
    return instancesDao;
  }
}
