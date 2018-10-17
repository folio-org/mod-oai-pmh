package org.folio.rest.impl;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Header;
import com.jayway.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.oaipmh.ResponseHelper;
import org.folio.rest.RestVerticle;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.client.test.HttpClientMock2;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openarchives.oai._2.GranularityType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.VerbType;

import javax.xml.bind.JAXBException;
import java.time.Instant;
import java.util.Properties;

import static com.jayway.restassured.RestAssured.given;
import static org.folio.rest.impl.OaiPmhImpl.REPOSITORY_ADMIN_EMAILS;
import static org.folio.rest.impl.OaiPmhImpl.REPOSITORY_BASE_URL;
import static org.folio.rest.impl.OaiPmhImpl.REPOSITORY_NAME;
import static org.folio.rest.impl.OaiPmhImpl.REPOSITORY_PROTOCOL_VERSION_2_0;

@RunWith(VertxUnitRunner.class)
public class OaiPmhImplTest {
  private final static Logger logger = LoggerFactory.getLogger(OaiPmhImplTest.class);

  // API paths
  private final String rootPath = "/oai";
  private final String listRecordsPath = rootPath + "/records";
  private final String listIdentifiersPath = rootPath + "/identifiers";
  private final String listMetadataFormatsPath = rootPath + "/metadata_formats";
  private final String listSetsPath = rootPath + "/sets";
  private final String identifyPath = rootPath + "/repository_info";

  private final Header tenantHeader = new Header("X-Okapi-Tenant", "diku");
  private final Header tokenHeader = new Header("X-Okapi-Token",
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkaWt1X2FkbWluIiwidXNlcl9pZCI6IjUwYjQyYmJmLTcwNzEtNTEwNi05NWQ2LTFkMjViZjczMjRmZiIsImlhdCI6MTUzODA0ODcyMiwidGVuYW50IjoiZGlrdSJ9.Qk3k_P9l025F-k3M-OoFJv6wPVAC7sepvgA8avEamZ8");
  private final Header contentTypeHeaderXML = new Header("Content-Type", "application/xml");

  private static Vertx vertx;

  private static int port;

  @BeforeClass
  public static void setUpOnce(TestContext context) {
    vertx = Vertx.vertx();
    String moduleName = PomReader.INSTANCE.getModuleName()
                                   .replaceAll("_", "-");  // RMB normalizes the dash to underscore, fix back
    String moduleVersion = PomReader.INSTANCE.getVersion();
    String moduleId = moduleName + "-" + moduleVersion;
    logger.info("Test setup starting for " + moduleId);

    port = NetworkUtils.nextFreePort();

    JsonObject conf = new JsonObject()
      .put("http.port", port)
      .put(HttpClientMock2.MOCK_MODE, "true");

    logger.info(String.format("mod-oai-pmh test: Deploying %s with %s", RestVerticle.class.getName(), Json.encode(conf)));

    DeploymentOptions opt = new DeploymentOptions().setConfig(conf);
    vertx.deployVerticle(RestVerticle.class.getName(), opt, context.asyncAssertSuccess());
    RestAssured.port = port;

    logger.info("mod-oai-pmh Test: setup done. Using port " + port);
  }

  @AfterClass
  public static void tearDownOnce(TestContext context) {
    logger.info("Cleaning up after mod-oai-pmh Test");
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> async.complete()));
  }

  @Test
  public void adminHealth(TestContext context) {
    Async async = context.async();

    // Simple GET request to see the module is running and we can talk to it.
    given()
      .get("/admin/health")
      .then()
      .log().all()
      .statusCode(200);

    async.complete();
  }

  @Test
  public void getOaiRecords(TestContext context) throws JAXBException {
    Async async = context.async();

    RequestSpecification requestSpecification = createBaseRequest();
    String response = test422WithXml(requestSpecification, listRecordsPath);

    // Check that error message is returned
    context.assertNotNull(response);

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh1FromString = ResponseHelper.getInstance().stringToOaiPmh(response);
    context.assertNotNull(oaipmh1FromString)
           .assertNotNull(oaipmh1FromString.getResponseDate())
           .assertTrue(oaipmh1FromString.getResponseDate().isBefore(Instant.now()))
           .assertNotNull(oaipmh1FromString.getRequest())
           .assertNotNull(oaipmh1FromString.getRequest().getValue())
           .assertEquals(VerbType.LIST_RECORDS, oaipmh1FromString.getRequest().getVerb())
           .assertNotNull(oaipmh1FromString.getErrors())
           .assertEquals(1, oaipmh1FromString.getErrors().size());
    async.complete();
  }

  @Test
  public void getOaiRecordsById(TestContext context) throws JAXBException {
    Async async = context.async();

    RequestSpecification requestSpecification = createBaseRequest();
    String response = test422WithXml(requestSpecification, listRecordsPath + "/someId");

    // Check that error message is returned
    context.assertNotNull(response);

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh1FromString = ResponseHelper.getInstance().stringToOaiPmh(response);
    context.assertNotNull(oaipmh1FromString)
           .assertNotNull(oaipmh1FromString.getResponseDate())
           .assertTrue(oaipmh1FromString.getResponseDate().isBefore(Instant.now()))
           .assertNotNull(oaipmh1FromString.getRequest())
           .assertNotNull(oaipmh1FromString.getRequest().getValue())
           .assertEquals(VerbType.GET_RECORD, oaipmh1FromString.getRequest().getVerb())
           .assertNotNull(oaipmh1FromString.getErrors())
           .assertEquals(1, oaipmh1FromString.getErrors().size());

    async.complete();
  }

  @Test
  public void getOaiIdentifiers(TestContext context) {
    Async async = context.async();

    RequestSpecification requestSpecification = createBaseRequest();
    String response = test500WithErrorMessage(requestSpecification, listIdentifiersPath);

    // Check that error message is returned
    context.assertNotNull(response);

    async.complete();
  }

  @Test
  public void getOaiMetadataFormats(TestContext context) {
    Async async = context.async();

    RequestSpecification requestSpecification = createBaseRequest();
    String response = test500WithErrorMessage(requestSpecification, listMetadataFormatsPath);

    // Check that error message is returned
    context.assertNotNull(response);

    async.complete();
  }

  @Test
  public void getOaiSets(TestContext context) {
    Async async = context.async();

    RequestSpecification requestSpecification = createBaseRequest();
    String response = test500WithErrorMessage(requestSpecification, listSetsPath);

    // Check that error message is returned
    context.assertNotNull(response);

    async.complete();
  }

  private static String test422WithXml(RequestSpecification requestSpecification, String endpoint) {
    return requestSpecification
      .when()
        .get(endpoint)
      .then()
        .statusCode(422)
      .contentType(ContentType.XML)
        .extract()
          .body()
            .asString();
  }

  private static String test500WithErrorMessage(RequestSpecification requestSpecification, String endpoint) {
    return requestSpecification
      .when()
        .get(endpoint)
      .then()
        .statusCode(500)
        .contentType(ContentType.TEXT)
        .extract()
          .body()
            .asString();
  }

  @Test
  public void getOaiRepositoryInfo(TestContext context) throws JAXBException {
    Async async = context.async();
    RequestSpecification requestSpecification = createBaseRequest();

    // Remove required props
    Properties sysProps = System.getProperties();
    sysProps.remove(REPOSITORY_NAME);
    sysProps.remove(REPOSITORY_BASE_URL);
    sysProps.remove(REPOSITORY_ADMIN_EMAILS);

    String response = test500WithErrorMessage(requestSpecification, identifyPath);
    // Check that error message is returned
    context.assertNotNull(response);


    // Set some required props but not all
    sysProps.setProperty(REPOSITORY_NAME, REPOSITORY_NAME);
    // Set 2 emails
    String emails = "oaiAdminEmail1@folio.org,oaiAdminEmail2@folio.org";
    sysProps.setProperty(REPOSITORY_ADMIN_EMAILS, emails);

    response = test500WithErrorMessage(requestSpecification, identifyPath);
    // Check that error message is returned
    context.assertNotNull(response);
    context.assertTrue(response.contains(emails));

    // Set all required system properties
    sysProps.setProperty(REPOSITORY_BASE_URL, REPOSITORY_BASE_URL);

    response = requestSpecification
      .when()
        .get(identifyPath)
      .then()
        .statusCode(200)
        .contentType(ContentType.XML)
        .extract()
          .body()
            .asString();
    // Check that error message is returned
    context.assertNotNull(response);

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh1FromString = ResponseHelper.getInstance().stringToOaiPmh(response);
    context.assertNotNull(oaipmh1FromString)
           .assertNotNull(oaipmh1FromString.getResponseDate())
           .assertTrue(oaipmh1FromString.getResponseDate().isBefore(Instant.now()))
           .assertNotNull(oaipmh1FromString.getRequest())
           .assertNotNull(oaipmh1FromString.getRequest().getValue())
           .assertEquals(VerbType.IDENTIFY, oaipmh1FromString.getRequest().getVerb())
           .assertNotNull(oaipmh1FromString.getIdentify())
           .assertNotNull(oaipmh1FromString.getIdentify().getBaseURL())
           .assertNotNull(oaipmh1FromString.getIdentify().getAdminEmails())
           .assertEquals(2, oaipmh1FromString.getIdentify().getAdminEmails().size())
           .assertNotNull(oaipmh1FromString.getIdentify().getEarliestDatestamp())
           .assertEquals(GranularityType.YYYY_MM_DD_THH_MM_SS_Z, oaipmh1FromString.getIdentify().getGranularity())
           .assertEquals(REPOSITORY_PROTOCOL_VERSION_2_0, oaipmh1FromString.getIdentify().getProtocolVersion())
           .assertNotNull(oaipmh1FromString.getIdentify().getRepositoryName());

    async.complete();
  }

  private RequestSpecification createBaseRequest() {
    return RestAssured
      .given()
        .header(tokenHeader)
        .header(tenantHeader)
        .header(contentTypeHeaderXML);
  }
}
