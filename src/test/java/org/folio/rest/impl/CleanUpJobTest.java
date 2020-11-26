package org.folio.rest.impl;

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
import org.folio.config.ApplicationConfig;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.liquibase.SingleConnectionProvider;
import org.folio.oaipmh.common.AbstractInstancesTest;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.rest.RestVerticle;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.spring.SpringContextUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;
import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.folio.rest.impl.OkapiMockServer.TEST_USER_ID;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@ExtendWith(VertxExtension.class)
@TestInstance(PER_CLASS)
public class CleanUpJobTest extends AbstractInstancesTest {

  private static final int okapiPort = NetworkUtils.nextFreePort();
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final String CLEAN_UP_INSTANCES_PATH = "/oai/clean-up-instances";

  private Header tenantHeader = new Header("X-Okapi-Tenant", OAI_TEST_TENANT);
  private Header okapiUrlHeader = new Header("X-Okapi-Url", "http://localhost:" + mockPort);
  private Header okapiUserHeader = new Header("X-Okapi-User-Id", TEST_USER_ID);

  @Autowired
  private InstancesDao instancesDao;

  @BeforeAll
  void setUpOnce(Vertx vertx, VertxTestContext testContext) throws Exception {
    String moduleName = PomReader.INSTANCE.getModuleName()
      .replaceAll("_", "-");
    String moduleVersion = PomReader.INSTANCE.getVersion();
    String moduleId = moduleName + "-" + moduleVersion;

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

  @Test
  void shouldReturn204AndClearExpiredInstances_whenThereExpiredRequestsExist(VertxTestContext testContext) {
    RequestSpecification request = createBaseRequest(CLEAN_UP_INSTANCES_PATH, null);
    request.when()
      .post()
      .then()
      .statusCode(204);
    verifyExpiredInstancesHasBeenCleared(testContext);
  }

  private void verifyExpiredInstancesHasBeenCleared(VertxTestContext testContext) {
    instancesDao.getInstancesList(0, 100, OAI_TEST_TENANT).onSuccess(instances -> {
      List<String> instancesIds = instances.stream().map(Instances::getInstanceId).map(UUID::toString).collect(Collectors.toList());
      assertFalse(instancesIds.contains(EXPIRED_INSTANCE_ID));
      testContext.completeNow();
    }).onFailure(testContext::failNow);
  }

  @Test
  void shouldReturn204AndDoNotClearInstances_whenThereNoAnyExpiredRequestIds(VertxTestContext testContext) {

  }

  @Override
  protected InstancesDao getInstancesDao() {
    return instancesDao;
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
