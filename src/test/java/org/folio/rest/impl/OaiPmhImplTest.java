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
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.ResponseHelper;
import org.folio.rest.RestVerticle;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.client.test.HttpClientMock2;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openarchives.oai._2.GranularityType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.VerbType;

import javax.xml.bind.JAXBException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.jayway.restassured.RestAssured.given;
import static org.folio.oaipmh.helpers.AbstractHelper.LIST_ILLEGAL_ARGUMENTS_ERROR;
import static org.folio.oaipmh.helpers.AbstractHelper.LIST_NO_REQUIRED_PARAM_ERROR;
import static org.folio.oaipmh.helpers.AbstractHelper.NO_RECORD_FOUND_ERROR;
import static org.folio.oaipmh.helpers.GetOaiRepositoryInfoHelper.REPOSITORY_ADMIN_EMAILS;
import static org.folio.oaipmh.helpers.GetOaiRepositoryInfoHelper.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.helpers.GetOaiRepositoryInfoHelper.REPOSITORY_NAME;
import static org.folio.oaipmh.helpers.GetOaiRepositoryInfoHelper.REPOSITORY_PROTOCOL_VERSION_2_0;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.ID_DOES_NOT_EXIST;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.NO_RECORDS_MATCH;
import static org.openarchives.oai._2.VerbType.GET_RECORD;
import static org.openarchives.oai._2.VerbType.IDENTIFY;
import static org.openarchives.oai._2.VerbType.LIST_IDENTIFIERS;
import static org.openarchives.oai._2.VerbType.LIST_METADATA_FORMATS;
import static org.openarchives.oai._2.VerbType.LIST_RECORDS;
import static org.openarchives.oai._2.VerbType.LIST_SETS;

@ExtendWith(VertxExtension.class)
class OaiPmhImplTest {
  private static final Logger logger = LoggerFactory.getLogger(OaiPmhImplTest.class);

  // API paths
  private static final String ROOT_PATH = "/oai";
  private static final String LIST_RECORDS_PATH = ROOT_PATH + "/records";
  private static final String LIST_IDENTIFIERS_PATH = ROOT_PATH + "/identifiers";
  private static final String LIST_METADATA_FORMATS_PATH = ROOT_PATH + "/metadata_formats";
  private static final String LIST_SETS_PATH = ROOT_PATH + "/sets";
  private static final String IDENTIFY_PATH = ROOT_PATH + "/repository_info";

  private static final int okapiPort = NetworkUtils.nextFreePort();
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final String APPLICATION_XML_TYPE = "application/xml";
  private static final String IDENTIFIER = "identifier";

  private final Header tenantHeader = new Header("X-Okapi-Tenant", "diku");
  private final Header tokenHeader = new Header("X-Okapi-Token", "eyJhbGciOiJIUzI1NiJ9");
  private final Header okapiUrlHeader = new Header("X-Okapi-Url", "http://localhost:" + mockPort);
  private final Header contentTypeHeaderXML = new Header("Content-Type", "application/xml");

  @BeforeAll
  static void setUpOnce(Vertx vertx, VertxTestContext testContext) {
    OkapiMockServer okapiMockServer = new OkapiMockServer(vertx, mockPort);

    String moduleName = PomReader.INSTANCE.getModuleName()
                                   .replaceAll("_", "-");  // RMB normalizes the dash to underscore, fix back
    String moduleVersion = PomReader.INSTANCE.getVersion();
    String moduleId = moduleName + "-" + moduleVersion;
    logger.info("Test setup starting for " + moduleId);

    JsonObject conf = new JsonObject()
      .put("http.port", okapiPort);

    logger.info(String.format("mod-oai-pmh test: Deploying %s with %s", RestVerticle.class.getName(), Json.encode(conf)));

    DeploymentOptions opt = new DeploymentOptions().setConfig(conf);

    vertx.deployVerticle(RestVerticle.class.getName(), opt, result -> {
      if (result.succeeded()) {
        okapiMockServer.start(testContext);
      } else {
        testContext.failNow(result.cause());
      }
    });
    RestAssured.baseURI = "http://localhost:" + okapiPort;
    RestAssured.port = okapiPort;
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

    logger.info("mod-oai-pmh Test: setup done. Using port " + okapiPort);
  }

  @Test
  void adminHealth(VertxTestContext testContext) {
    // Simple GET request to see the module is running and we can talk to it.
    given()
      .get("/admin/health")
      .then()
      .log().all()
      .statusCode(200);

    testContext.completeNow();
  }

  @Test
  void getOaiRecords(VertxTestContext testContext) throws JAXBException {
    RequestSpecification requestSpecification = createBaseRequest();
    String response = test422WithXml(requestSpecification, LIST_RECORDS_PATH);

    // Check that error message is returned
    assertThat(response, is(notNullValue()));

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmhFromString = ResponseHelper.getInstance().stringToOaiPmh(response);
    verifyBaseResponse(oaipmhFromString, LIST_RECORDS);
    assertThat(oaipmhFromString.getErrors(), is(notNullValue()));
    assertThat(oaipmhFromString.getErrors(), hasSize(equalTo(1)));

    testContext.completeNow();
  }

  @Test
  void getOaiRecordsById(VertxTestContext testContext) throws JAXBException {
    RequestSpecification requestSpecification = createBaseRequest();
    String response = test422WithXml(requestSpecification, LIST_RECORDS_PATH + "/someId");

    // Check that error message is returned
    assertThat(response, is(notNullValue()));

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmhFromString = ResponseHelper.getInstance().stringToOaiPmh(response);
    verifyBaseResponse(oaipmhFromString, GET_RECORD);
    assertThat(oaipmhFromString.getErrors(), is(notNullValue()));
    assertThat(oaipmhFromString.getErrors(), hasSize(equalTo(1)));

    testContext.completeNow();
  }

  @Test
  void getOaiIdentifiersSuccess(VertxTestContext testContext) throws JAXBException {
    RequestSpecification requestSpecification = createBaseRequest();

    String response = requestSpecification
      .when()
      .get(LIST_IDENTIFIERS_PATH + "?metadataPrefix=" + MetadataPrefix.MARC_XML.getName())
      .then()
      .statusCode(200)
      .contentType(ContentType.XML)
      .extract()
      .body()
      .asString();

    // Check that error message is returned
    assertThat(response, is(notNullValue()));

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh = ResponseHelper.getInstance().stringToOaiPmh(response);

    verifyBaseResponse(oaipmh, LIST_IDENTIFIERS);
    assertThat(oaipmh.getErrors(), is(empty()));
    assertThat(oaipmh.getListIdentifiers(), is(notNullValue()));
    assertThat(oaipmh.getListIdentifiers().getHeaders(), hasSize(10));

    testContext.completeNow();
  }

  @Test
  void getOaiIdentifiersWithoutParams(VertxTestContext testContext) throws JAXBException {
    List<OAIPMHerrorType> errors = verifyListIdentifiersErrors(LIST_IDENTIFIERS_PATH, 1).getErrors();
    OAIPMHerrorType error = errors.get(0);
    assertThat(error.getCode(), equalTo(BAD_ARGUMENT));
    assertThat(error.getValue(), equalTo(LIST_NO_REQUIRED_PARAM_ERROR));

    testContext.completeNow();
  }

  @Test
  void getOaiIdentifiersWithWrongMetadataPrefix(VertxTestContext testContext) throws JAXBException {
    String metadataPrefix = "abc";
    OAIPMH oaipmh = verifyListIdentifiersErrors(LIST_IDENTIFIERS_PATH + "?metadataPrefix=" + metadataPrefix, 1);

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix));

    List<OAIPMHerrorType> errors = oaipmh.getErrors();
    assertThat(errors.get(0).getCode(), equalTo(CANNOT_DISSEMINATE_FORMAT));

    testContext.completeNow();
  }

  @Test
  void getOaiIdentifiersWithResumptionToken(VertxTestContext testContext) throws JAXBException {
    String resumptionToken = "abc";
    OAIPMH oaipmh = verifyListIdentifiersErrors(LIST_IDENTIFIERS_PATH + "?resumptionToken=" + resumptionToken, 1);
    assertThat(oaipmh.getRequest().getResumptionToken(), equalTo(resumptionToken));

    List<OAIPMHerrorType> errors = oaipmh.getErrors();
    assertThat(errors.get(0).getCode(), equalTo(BAD_RESUMPTION_TOKEN));

    testContext.completeNow();
  }

  @Test
  void getOaiIdentifiersWithResumptionTokenAndWrongMetadataPrefix(VertxTestContext testContext) throws JAXBException {
    String resumptionToken = "abc";
    String metadataPrefix = "marc";
    String endpoint = String.format(LIST_IDENTIFIERS_PATH + "?resumptionToken=%s&metadataPrefix=%s", resumptionToken, metadataPrefix);

    OAIPMH oaipmh = verifyListIdentifiersErrors(endpoint, 3);
    assertThat(oaipmh.getRequest().getResumptionToken(), equalTo(resumptionToken));
    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix));

    List<OAIPMHerrorType> errors = oaipmh.getErrors();
    List<OAIPMHerrorcodeType> codes = errors.stream()
                                            .map(OAIPMHerrorType::getCode)
                                            .collect(Collectors.toList());
    assertThat(codes, containsInAnyOrder(BAD_RESUMPTION_TOKEN, BAD_ARGUMENT, CANNOT_DISSEMINATE_FORMAT));

    Optional<String> badArgMsg = errors.stream().filter(error -> error.getCode() == BAD_ARGUMENT).map(OAIPMHerrorType::getValue).findAny();
    badArgMsg.ifPresent(msg -> assertThat(msg, equalTo(LIST_ILLEGAL_ARGUMENTS_ERROR)));

    testContext.completeNow();
  }

  @Test
  void getOaiIdentifiersWithWrongDatesAndWrongSet(VertxTestContext testContext) throws JAXBException {
    String metadataPrefix = MetadataPrefix.MARC_XML.getName();
    String from = "2018-09-19T02:52:08.873+0000";
    String until = "2018-10-20T02:03:04.567+0000";
    String set = "single";
    String endpoint = String.format(LIST_IDENTIFIERS_PATH + "?metadataPrefix=%s&from=%s&until=%s&set=%s", metadataPrefix, from, until, set);

    OAIPMH oaipmh = verifyListIdentifiersErrors(endpoint, 3);

    assertThat(oaipmh.getRequest().getSet(), equalTo(set));
    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix));

    // The dates are of invalid format so they are not present in request
    assertThat(oaipmh.getRequest().getFrom(), nullValue());
    assertThat(oaipmh.getRequest().getUntil(), nullValue());

    List<OAIPMHerrorType> errors = oaipmh.getErrors();
    List<OAIPMHerrorcodeType> codes = errors.stream()
                                            .map(OAIPMHerrorType::getCode)
                                            .collect(Collectors.toList());
    assertThat(codes, containsInAnyOrder(BAD_ARGUMENT, BAD_ARGUMENT, NO_RECORDS_MATCH));
    Optional<String> noRecordsMsg = errors.stream().filter(error -> error.getCode() == NO_RECORDS_MATCH).map(OAIPMHerrorType::getValue).findAny();
    noRecordsMsg.ifPresent(msg -> assertThat(msg, equalTo(NO_RECORD_FOUND_ERROR)));

    testContext.completeNow();
  }

  private OAIPMH verifyListIdentifiersErrors(String endpoint, int errorsCount) throws JAXBException {
    RequestSpecification requestSpecification = createBaseRequest();

    String response = requestSpecification
      .when()
      .get(endpoint)
      .then()
      .statusCode(404)
      .contentType(ContentType.XML)
      .extract()
      .body()
      .asString();

    // Check that error message is returned
    assertThat(response, is(notNullValue()));

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmhFromString = ResponseHelper.getInstance().stringToOaiPmh(response);

    verifyBaseResponse(oaipmhFromString, LIST_IDENTIFIERS);
    assertThat(oaipmhFromString.getErrors(), is(notNullValue()));
    assertThat(oaipmhFromString.getErrors(), hasSize(errorsCount));

    return oaipmhFromString;
  }

  @Test
  void getOaiMetadataFormats(VertxTestContext testContext) throws JAXBException {
    logger.info("=== Test Metadata Formats without identifier ===");

    String responseWithoutIdentifier = createBaseRequest()
      .get(LIST_METADATA_FORMATS_PATH)
      .then()
      .contentType(APPLICATION_XML_TYPE)
      .statusCode(200)
      .extract()
      .response().asString();

    assertThat(responseWithoutIdentifier, is(notNullValue()));

    logger.info("Response with request without identifier: " + responseWithoutIdentifier);

    OAIPMH oaiPmhResponseWithoutIdentifier = ResponseHelper.getInstance().stringToOaiPmh(responseWithoutIdentifier);
    verifyBaseResponse(oaiPmhResponseWithoutIdentifier, LIST_METADATA_FORMATS);

    assertThat(oaiPmhResponseWithoutIdentifier.getListMetadataFormats(), is(notNullValue()));
    assertThat(oaiPmhResponseWithoutIdentifier.getErrors(), is(empty()));

    testContext.completeNow();
  }

  @Test
  void getOaiMetadataFormatsWithExistingIdentifier(VertxTestContext testContext) throws JAXBException {
    logger.info("=== Test Metadata Formats with existing identifier ===");

    String responseWithExistingIdentifier = createBaseRequest()
      .with()
      .param(IDENTIFIER, OkapiMockServer.EXISTING_IDENTIFIER)
      .get(LIST_METADATA_FORMATS_PATH)
      .then()
      .contentType(APPLICATION_XML_TYPE)
      .statusCode(200)
      .extract()
      .body()
      .asString();

    logger.info("Response with request with existing identifier: " + responseWithExistingIdentifier);

    OAIPMH oaiPmhResponseWithExistingIdentifier = ResponseHelper.getInstance().stringToOaiPmh(responseWithExistingIdentifier);
    verifyBaseResponse(oaiPmhResponseWithExistingIdentifier, LIST_METADATA_FORMATS);

    assertThat(oaiPmhResponseWithExistingIdentifier.getListMetadataFormats(), is(notNullValue()));
    assertThat(oaiPmhResponseWithExistingIdentifier.getErrors(), is(empty()));

    testContext.completeNow();
  }

  @Test
  void getOaiMetadataFormatsWithNonExistingIdentifier(VertxTestContext testContext) throws JAXBException {
    logger.info("=== Test Metadata Formats with non-existing identifier ===");

    // Check that error message is returned
    String responseWithNonExistingIdentifier = createBaseRequest()
      .with()
      .param(IDENTIFIER, OkapiMockServer.NON_EXISTING_IDENTIFIER)
      .get(LIST_METADATA_FORMATS_PATH)
      .then()
      .contentType(APPLICATION_XML_TYPE)
      .statusCode(404)
      .extract()
      .body()
      .asString();

    logger.info("Response with request with non-existing identifier: " + responseWithNonExistingIdentifier);

    OAIPMH oaiPmhResponseWithNonExistingIdentifier = ResponseHelper.getInstance().stringToOaiPmh(responseWithNonExistingIdentifier);
    verifyBaseResponse(oaiPmhResponseWithNonExistingIdentifier, LIST_METADATA_FORMATS);

    assertThat(oaiPmhResponseWithNonExistingIdentifier.getListMetadataFormats(), is(nullValue()));
    assertThat(oaiPmhResponseWithNonExistingIdentifier.getErrors(), hasSize(1));
    assertThat(oaiPmhResponseWithNonExistingIdentifier.getErrors().get(0).getCode(), equalTo(ID_DOES_NOT_EXIST));
    assertThat(oaiPmhResponseWithNonExistingIdentifier.getErrors().get(0).getValue(), equalTo("Identifier not found"));

    testContext.completeNow();
  }

  @Test
  void testSuccessfulGetOaiSets(VertxTestContext testContext) throws JAXBException {
    RequestSpecification requestSpecification = createBaseRequest();

    String response = requestSpecification
      .when()
        .get(LIST_SETS_PATH)
      .then()
        .statusCode(200)
        .contentType(ContentType.XML)
        .extract()
          .body().asString();

    // Check that error message is returned
    assertThat(response, is(notNullValue()));

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmhFromString = ResponseHelper.getInstance().stringToOaiPmh(response);

    verifyBaseResponse(oaipmhFromString, LIST_SETS);
    assertThat(oaipmhFromString.getListSets(), is(notNullValue()));
    assertThat(oaipmhFromString.getListSets().getSets(), hasSize(equalTo(1)));
    assertThat(oaipmhFromString.getListSets().getSets().get(0).getSetSpec(), equalTo("all"));
    assertThat(oaipmhFromString.getListSets().getSets().get(0).getSetName(), equalTo("All records"));

    testContext.completeNow();
  }

  @Test
  void testGetOaiSetsMissingBaseUrlProperty(VertxTestContext testContext) {

    // Remove required props
    String baseUrl = System.getProperty(REPOSITORY_BASE_URL);
    try {
      RequestSpecification requestSpecification = createBaseRequest();
      System.getProperties().remove(REPOSITORY_BASE_URL);

      String response = test500WithErrorMessage(requestSpecification, LIST_SETS_PATH);
      // Check that error message is returned
      assertThat(response, is(notNullValue()));
      assertThat(response, is(equalTo("Sorry, we can't process your request. Please contact administrator(s).")));

      testContext.completeNow();
    } finally {
      System.setProperty(REPOSITORY_BASE_URL, baseUrl);
    }
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
  void getOaiRepositoryInfo(VertxTestContext testContext) throws JAXBException {
    RequestSpecification requestSpecification = createBaseRequest();

    // Remove required props
    Properties sysProps = System.getProperties();
    sysProps.remove(REPOSITORY_NAME);
    sysProps.remove(REPOSITORY_BASE_URL);
    sysProps.remove(REPOSITORY_ADMIN_EMAILS);

    String response = test500WithErrorMessage(requestSpecification, IDENTIFY_PATH);
    // Check that error message is returned
    assertThat(response, is(notNullValue()));


    // Set some required props but not all
    sysProps.setProperty(REPOSITORY_NAME, REPOSITORY_NAME);
    // Set 2 emails
    String emails = "oaiAdminEmail1@folio.org,oaiAdminEmail2@folio.org";
    sysProps.setProperty(REPOSITORY_ADMIN_EMAILS, emails);

    response = test500WithErrorMessage(requestSpecification, IDENTIFY_PATH);
    // Check that error message is returned
    assertThat(response, is(notNullValue()));
    assertThat(response, is(equalTo("Sorry, we can't process your request. Please contact administrator(s).")));

    // Set all required system properties
    sysProps.setProperty(REPOSITORY_BASE_URL, REPOSITORY_BASE_URL);

    response = requestSpecification
      .when()
        .get(IDENTIFY_PATH)
      .then()
        .statusCode(200)
        .contentType(ContentType.XML)
        .extract()
          .body()
            .asString();
    // Check that error message is returned
    assertThat(response, is(notNullValue()));

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmhFromString = ResponseHelper.getInstance().stringToOaiPmh(response);

    verifyBaseResponse(oaipmhFromString, IDENTIFY);
    assertThat(oaipmhFromString.getIdentify(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getBaseURL(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getAdminEmails(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getAdminEmails(), hasSize(equalTo(2)));
    assertThat(oaipmhFromString.getIdentify().getEarliestDatestamp(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getGranularity(), is(equalTo(GranularityType.YYYY_MM_DD_THH_MM_SS_Z)));
    assertThat(oaipmhFromString.getIdentify().getProtocolVersion(), is(equalTo(REPOSITORY_PROTOCOL_VERSION_2_0)));
    assertThat(oaipmhFromString.getIdentify().getRepositoryName(), is(notNullValue()));

    testContext.completeNow();
  }

  private RequestSpecification createBaseRequest() {
    return RestAssured
      .given()
        .header(okapiUrlHeader)
        .header(tokenHeader)
        .header(tenantHeader)
        .header(contentTypeHeaderXML)
        .contentType(APPLICATION_XML_TYPE);
  }

  private void verifyBaseResponse(OAIPMH oaipmhFromString, VerbType verb) {
    assertThat(oaipmhFromString, is(notNullValue()));
    assertThat(oaipmhFromString.getResponseDate(), is(notNullValue()));
    assertThat(oaipmhFromString.getResponseDate().isBefore(Instant.now()), is(true));
    assertThat(oaipmhFromString.getRequest(), is(notNullValue()));
    assertThat(oaipmhFromString.getRequest().getValue(), is(notNullValue()));
    assertThat(oaipmhFromString.getRequest().getVerb(), equalTo(verb));
  }
}
