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
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.openarchives.oai._2.GranularityType;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.VerbType;

import javax.xml.bind.JAXBException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.jayway.restassured.RestAssured.given;
import static org.folio.oaipmh.Constants.FROM_PARAM;
import static org.folio.oaipmh.Constants.IDENTIFIER_PARAM;
import static org.folio.oaipmh.Constants.LIST_ILLEGAL_ARGUMENTS_ERROR;
import static org.folio.oaipmh.Constants.LIST_NO_REQUIRED_PARAM_ERROR;
import static org.folio.oaipmh.Constants.METADATA_PREFIX_PARAM;
import static org.folio.oaipmh.Constants.NO_RECORD_FOUND_ERROR;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_PARAM;
import static org.folio.oaipmh.Constants.SET_PARAM;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.Constants.DEFLATE;
import static org.folio.oaipmh.Constants.FROM_PARAM;
import static org.folio.oaipmh.Constants.GZIP;
import static org.folio.oaipmh.Constants.IDENTIFIER_PARAM;
import static org.folio.oaipmh.Constants.LIST_ILLEGAL_ARGUMENTS_ERROR;
import static org.folio.oaipmh.Constants.LIST_NO_REQUIRED_PARAM_ERROR;
import static org.folio.oaipmh.Constants.METADATA_PREFIX_PARAM;
import static org.folio.oaipmh.Constants.NO_RECORD_FOUND_ERROR;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_PARAM;
import static org.folio.oaipmh.Constants.SET_PARAM;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.helpers.GetOaiRepositoryInfoHelper.REPOSITORY_ADMIN_EMAILS;
import static org.folio.oaipmh.helpers.GetOaiRepositoryInfoHelper.REPOSITORY_NAME;
import static org.folio.oaipmh.helpers.GetOaiRepositoryInfoHelper.REPOSITORY_PROTOCOL_VERSION_2_0;
import static org.folio.rest.impl.OkapiMockServer.INVALID_IDENTIFIER;
import static org.folio.rest.impl.OkapiMockServer.THREE_INSTANCES_DATE;
import static org.hamcrest.CoreMatchers.containsString;
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
  private static final String GET_RECORD_PATH = LIST_RECORDS_PATH + "/{identifier}";
  private static final String LIST_IDENTIFIERS_PATH = ROOT_PATH + "/identifiers";
  private static final String LIST_METADATA_FORMATS_PATH = ROOT_PATH + "/metadata_formats";
  private static final String LIST_SETS_PATH = ROOT_PATH + "/sets";
  private static final String IDENTIFY_PATH = ROOT_PATH + "/repository_info";

  private static final int okapiPort = NetworkUtils.nextFreePort();
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final String APPLICATION_XML_TYPE = "application/xml";
  private static final String TENANT = "diku";
  private static final String IDENTIFIER_PREFIX = "oai:test.folio.org:" + TENANT + "/";

  private final Header tenantHeader = new Header("X-Okapi-Tenant", TENANT);
  private final Header tokenHeader = new Header("X-Okapi-Token", "eyJhbGciOiJIUzI1NiJ9");
  private final Header okapiUrlHeader = new Header("X-Okapi-Url", "http://localhost:" + mockPort);

  private static final Map<VerbType, String> basePaths = new HashMap<>();

  static {
    basePaths.put(GET_RECORD, GET_RECORD_PATH);
    basePaths.put(LIST_RECORDS, LIST_RECORDS_PATH);
    basePaths.put(LIST_IDENTIFIERS, LIST_IDENTIFIERS_PATH);
    basePaths.put(LIST_METADATA_FORMATS, LIST_METADATA_FORMATS_PATH);
    basePaths.put(LIST_SETS, LIST_SETS_PATH);
    basePaths.put(IDENTIFY, IDENTIFY_PATH);
  }

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

    vertx.deployVerticle(RestVerticle.class.getName(), opt, testContext.succeeding(id -> {
      OaiPmhImpl.init(testContext.succeeding(success -> {
        RestAssured.baseURI = "http://localhost:" + okapiPort;
        RestAssured.port = okapiPort;
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

        logger.info("mod-oai-pmh Test: setup done. Using port " + okapiPort);
        // Once MockServer starts, it indicates to junit that process is finished by calling context.completeNow()
        okapiMockServer.start(testContext);
      }));
    }));
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
  void getOaiIdentifiersSuccess() throws JAXBException {
    RequestSpecification request = createBaseRequest(LIST_IDENTIFIERS_PATH)
      .with()
        .param(METADATA_PREFIX_PARAM, MetadataPrefix.MARC_XML.getName());

    OAIPMH oaipmh = verify200WithXml(request, LIST_IDENTIFIERS);

    assertThat(oaipmh.getErrors(), is(empty()));
    assertThat(oaipmh.getListIdentifiers(), is(notNullValue()));
    assertThat(oaipmh.getListIdentifiers().getHeaders(), hasSize(10));

    oaipmh.getListIdentifiers().getHeaders().forEach(this::verifyHeader);
  }

  @ParameterizedTest
  @EnumSource(value = MetadataPrefix.class, names = { "MARC_XML", "DC" })
  void getOaiIdentifiersWithDateRange(MetadataPrefix prefix) throws JAXBException {
    OAIPMH oaipmh = verifyOaiListVerbWithDateRange(LIST_IDENTIFIERS, prefix);

    assertThat(oaipmh.getListIdentifiers(), is(notNullValue()));
    assertThat(oaipmh.getListIdentifiers().getHeaders(), hasSize(3));

    oaipmh.getListIdentifiers()
          .getHeaders()
          .forEach(this::verifyHeader);
  }

  @ParameterizedTest
  @EnumSource(value = MetadataPrefix.class, names = { "MARC_XML", "DC" })
  void getOaiRecordsWithDateRange(MetadataPrefix prefix) throws JAXBException {
    OAIPMH oaipmh = verifyOaiListVerbWithDateRange(LIST_RECORDS, prefix);

    assertThat(oaipmh.getListRecords(), is(notNullValue()));
    assertThat(oaipmh.getListRecords().getRecords(), hasSize(3));

    oaipmh.getListRecords()
          .getRecords()
          .forEach(record -> {
            assertThat(record.getMetadata(), is(notNullValue()));
            verifyHeader(record.getHeader());
          });
  }

  private OAIPMH verifyOaiListVerbWithDateRange(VerbType verb, MetadataPrefix prefix) throws JAXBException {
    String metadataPrefix = prefix.getName();
    String from = "2018-09-19T02:52:08Z";
    String until = THREE_INSTANCES_DATE;
    String set = "all";

    RequestSpecification request = createBaseRequest(basePaths.get(verb))
      .with()
      .param(FROM_PARAM, from)
      .param(UNTIL_PARAM, until)
      .param(METADATA_PREFIX_PARAM, metadataPrefix)
      .param(SET_PARAM, set);

    OAIPMH oaipmh = verify200WithXml(request, verb);

    assertThat(oaipmh.getErrors(), is(empty()));

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix));
    assertThat(oaipmh.getRequest().getFrom(), equalTo(from));
    assertThat(oaipmh.getRequest().getUntil(), equalTo(until));
    assertThat(oaipmh.getRequest().getSet(), equalTo(set));

    return oaipmh;
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS"})
  void getOaiListVerbWithoutParams(VerbType verb) throws JAXBException {
    List<OAIPMHerrorType> errors = verifyResponseWithErrors(createBaseRequest(basePaths.get(verb)), verb, 400, 1).getErrors();
    OAIPMHerrorType error = errors.get(0);
    assertThat(error.getCode(), equalTo(BAD_ARGUMENT));
    assertThat(error.getValue(), equalTo(LIST_NO_REQUIRED_PARAM_ERROR));
  }



  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiListVerbWithWrongMetadataPrefix(VerbType verb) throws JAXBException {
    String metadataPrefix = "abc";
    RequestSpecification request = createBaseRequest(basePaths.get(verb))
      .with()
        .param(METADATA_PREFIX_PARAM, metadataPrefix);

    OAIPMH oaipmh = verifyResponseWithErrors(request, verb, 422, 1);

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix));

    List<OAIPMHerrorType> errors = oaipmh.getErrors();
    assertThat(errors.get(0).getCode(), equalTo(CANNOT_DISSEMINATE_FORMAT));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiListVerbWithResumptionToken(VerbType verb) throws JAXBException {
    String resumptionToken = "abc";
    RequestSpecification request = createBaseRequest(basePaths.get(verb))
      .with()
        .param(RESUMPTION_TOKEN_PARAM, resumptionToken);

    OAIPMH oaipmh = verifyResponseWithErrors(request, verb, 400, 1);

    assertThat(oaipmh.getRequest().getResumptionToken(), equalTo(resumptionToken));

    List<OAIPMHerrorType> errors = oaipmh.getErrors();
    assertThat(errors.get(0).getCode(), equalTo(BAD_RESUMPTION_TOKEN));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiListVerbWithResumptionTokenAndWrongMetadataPrefix(VerbType verb) throws JAXBException {
    String resumptionToken = "abc";
    String metadataPrefix = "marc";
    RequestSpecification request = createBaseRequest(basePaths.get(verb))
      .with()
        .param(METADATA_PREFIX_PARAM, metadataPrefix)
        .param(RESUMPTION_TOKEN_PARAM, resumptionToken);

    OAIPMH oaipmh = verifyResponseWithErrors(request, verb, 400, 3);

    assertThat(oaipmh.getRequest().getResumptionToken(), equalTo(resumptionToken));
    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix));

    List<OAIPMHerrorType> errors = oaipmh.getErrors();
    List<OAIPMHerrorcodeType> codes = errors.stream()
                                            .map(OAIPMHerrorType::getCode)
                                            .collect(Collectors.toList());
    assertThat(codes, containsInAnyOrder(BAD_RESUMPTION_TOKEN, BAD_ARGUMENT, CANNOT_DISSEMINATE_FORMAT));

    Optional<String> badArgMsg = errors.stream().filter(error -> error.getCode() == BAD_ARGUMENT).map(OAIPMHerrorType::getValue).findAny();
    badArgMsg.ifPresent(msg -> assertThat(msg, equalTo(LIST_ILLEGAL_ARGUMENTS_ERROR)));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiListVerbWithWrongSet(VerbType verb) throws JAXBException {
    String metadataPrefix = MetadataPrefix.MARC_XML.getName();
    String set = "single";

    RequestSpecification request = createBaseRequest(basePaths.get(verb))
      .with()
        .param(METADATA_PREFIX_PARAM, metadataPrefix)
        .param(SET_PARAM, set);

    OAIPMH oaipmh = verifyResponseWithErrors(request, verb, 404, 1);

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix));
    assertThat(oaipmh.getRequest().getSet(), equalTo(set));

    OAIPMHerrorType error = oaipmh.getErrors().get(0);
    assertThat(error.getCode(), equalTo(NO_RECORDS_MATCH));
    assertThat(error.getValue(), equalTo(NO_RECORD_FOUND_ERROR));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiListVerbWithWrongDatesAndWrongSet(VerbType verb) throws JAXBException {
    String metadataPrefix = MetadataPrefix.MARC_XML.getName();
    String from = "2018-09-19T02:52:08.873";
    String until = "2018-10-20T02:03:04.567";
    String set = "single";

    RequestSpecification request = createBaseRequest(basePaths.get(verb))
      .with()
        .param(FROM_PARAM, from)
        .param(UNTIL_PARAM, until)
        .param(METADATA_PREFIX_PARAM, metadataPrefix)
        .param(SET_PARAM, set);

    OAIPMH oaipmh = verifyResponseWithErrors(request, verb, 400, 3);

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
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiListVerbWithInvalidDateRange(VerbType verb) throws JAXBException {
    String metadataPrefix = MetadataPrefix.MARC_XML.getName();
    String from = "2018-12-19T02:52:08Z";
    String until = "2018-10-20T02:03:04Z";

    RequestSpecification request = createBaseRequest(basePaths.get(verb))
      .with()
        .param(FROM_PARAM, from)
        .param(UNTIL_PARAM, until)
        .param(METADATA_PREFIX_PARAM, metadataPrefix);

    OAIPMH oaipmh = verifyResponseWithErrors(request, verb, 400, 1);

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix));
    assertThat(oaipmh.getRequest().getFrom(), equalTo(from));
    assertThat(oaipmh.getRequest().getUntil(), equalTo(until));

    OAIPMHerrorType error = oaipmh.getErrors().get(0);
    assertThat(error.getCode(), equalTo(BAD_ARGUMENT));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiListVerbWithNoRecordsFoundFromStorage(VerbType verb) throws JAXBException {
    String metadataPrefix = MetadataPrefix.DC.getName();
    String from = OkapiMockServer.NO_RECORDS_DATE;
    String set = "all";

    RequestSpecification request = createBaseRequest(basePaths.get(verb))
      .with()
        .param(FROM_PARAM, from)
        .param(SET_PARAM, set)
        .param(METADATA_PREFIX_PARAM, metadataPrefix);

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh = verifyResponseWithErrors(request, verb, 404, 1);

    // The dates are of invalid format so they are not present in request
    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix));
    assertThat(oaipmh.getRequest().getFrom(), equalTo(from));
    assertThat(oaipmh.getRequest().getSet(), equalTo(set));

    OAIPMHerrorType error = oaipmh.getErrors().get(0);
    assertThat(error.getCode(), equalTo(NO_RECORDS_MATCH));
    assertThat(error.getValue(), equalTo(NO_RECORD_FOUND_ERROR));
  }

  @ParameterizedTest
  @EnumSource(value = MetadataPrefix.class, names = { "MARC_XML", "DC" })
  void getOaiListRecordsVerbWithOneNotFoundRecordFromStorage(MetadataPrefix metadataPrefix) throws JAXBException {
    String from = OkapiMockServer.DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOT_RECORD;
    RequestSpecification request = createBaseRequest(LIST_RECORDS_PATH)
      .with()
      .param(FROM_PARAM, from)
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh = verify200WithXml(request, LIST_RECORDS);

    // The dates are of invalid format so they are not present in request
    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix.getName()));
    assertThat(oaipmh.getRequest().getFrom(), equalTo(from));

    assertThat(oaipmh.getListRecords(), is(notNullValue()));
    assertThat(oaipmh.getListRecords().getRecords(), hasSize(3));

    oaipmh.getListRecords()
          .getRecords()
          .forEach(record -> {
            assertThat(record.getMetadata(), is(notNullValue()));
            verifyHeader(record.getHeader());
          });
  }

  @ParameterizedTest
  @EnumSource(value = MetadataPrefix.class, names = { "MARC_XML", "DC" })
  void getOaiListRecordsVerbWithErrorFromRecordStorage(MetadataPrefix metadataPrefix) {
    RequestSpecification request = createBaseRequest(LIST_RECORDS_PATH)
      .with()
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName())
      .param(UNTIL_PARAM, OkapiMockServer.RECORD_STORAGE_INTERNAL_SERVER_ERROR_DATE);

    verify500WithErrorMessage(request);
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiIdentifiersWithErrorFromStorage(VerbType verb) {
    RequestSpecification request = createBaseRequest(basePaths.get(verb))
      .with()
        .param(METADATA_PREFIX_PARAM, MetadataPrefix.DC.getName())
        .param(UNTIL_PARAM, OkapiMockServer.ERROR_DATE);

    verify500WithErrorMessage(request);
  }

  @ParameterizedTest
  @EnumSource(value = MetadataPrefix.class, names = { "MARC_XML", "DC" })
  void getOaiRecordsByIdInvalidIdentifier(MetadataPrefix metadataPrefix) throws JAXBException {
    RequestSpecification requestSpecification = createBaseRequest(GET_RECORD_PATH)
      .pathParam(IDENTIFIER_PARAM, INVALID_IDENTIFIER)
      .with()
      .param(METADATA_PREFIX_PARAM,metadataPrefix.getName());
    String response = verifyWithCodeWithXml(requestSpecification, 400);

    // Check that error message is returned
    assertThat(response, is(notNullValue()));

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh = ResponseHelper.getInstance().stringToOaiPmh(response);
    verifyBaseResponse(oaipmh, GET_RECORD);
    assertThat(oaipmh.getGetRecord(), is(nullValue()));
    assertThat(oaipmh.getErrors().get(0).getCode(), equalTo(BAD_ARGUMENT));
  }

  @ParameterizedTest
  @EnumSource(value = MetadataPrefix.class, names = { "MARC_XML", "DC" })
  void getOaiListRecordsVerbWithOneInstanceButNotFoundRecordFromStorage(MetadataPrefix metadataPrefix) throws JAXBException {
    String from = OkapiMockServer.DATE_FOR_ONE_INSTANCE_BUT_WITHOT_RECORD;
    RequestSpecification request = createBaseRequest(LIST_RECORDS_PATH)
      .with()
      .param(FROM_PARAM, from)
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());

    OAIPMH oaipmh = verifyResponseWithErrors(request, LIST_RECORDS, 404, 1);

    // The dates are of invalid format so they are not present in request
    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix.getName()));
    assertThat(oaipmh.getRequest().getFrom(), equalTo(from));

    OAIPMHerrorType error = oaipmh.getErrors().get(0);
    assertThat(error.getCode(), equalTo(NO_RECORDS_MATCH));
    assertThat(error.getValue(), equalTo(NO_RECORD_FOUND_ERROR));
  }

  @ParameterizedTest
  @EnumSource(value = MetadataPrefix.class, names = { "MARC_XML", "DC" })
  void getOaiGetRecordVerbWithOneInstanceButNotFoundRecordFromStorage(MetadataPrefix metadataPrefix) throws JAXBException {
    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.NOT_FOUND_RECORD_INSTANCE_ID;
    RequestSpecification request = createBaseRequest(GET_RECORD_PATH)
      .with()
      .pathParam(IDENTIFIER_PARAM, identifier)
      .with()
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());

    OAIPMH oaipmh = verifyResponseWithErrors(request, GET_RECORD, 404, 1);

    // The dates are of invalid format so they are not present in request
    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix.getName()));
    assertThat(oaipmh.getRequest().getIdentifier(), equalTo(identifier));

    OAIPMHerrorType error = oaipmh.getErrors().get(0);
    assertThat(error.getCode(), equalTo(ID_DOES_NOT_EXIST));
  }

  @Test
  void getOaiGetRecordVerbWithWrongMetadataPrefix() throws JAXBException {
    String metadataPrefix = "mark_xml";
    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.EXISTING_IDENTIFIER;
    RequestSpecification request = createBaseRequest(GET_RECORD_PATH)
      .with()
      .pathParam(IDENTIFIER_PARAM, identifier)
      .with()
      .param(METADATA_PREFIX_PARAM, metadataPrefix);
    OAIPMH oaipmh = verifyResponseWithErrors(request, GET_RECORD, 422, 1);
    assertThat(oaipmh.getGetRecord(), is(nullValue()));
    assertThat(oaipmh.getErrors().get(0).getCode(), equalTo(CANNOT_DISSEMINATE_FORMAT));
  }

  @Test
  void getOaiGetRecordVerbWithoutMetadataPrefix(VertxTestContext testContext) throws JAXBException {
    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.EXISTING_IDENTIFIER;
    RequestSpecification request = createBaseRequest(GET_RECORD_PATH)
      .with().pathParam(IDENTIFIER_PARAM, identifier);
    OAIPMH oaipmh = verifyResponseWithErrors(request, GET_RECORD, 400, 1);
    assertThat(oaipmh.getGetRecord(), is(nullValue()));
    assertThat(oaipmh.getErrors().get(0).getCode(), equalTo(BAD_ARGUMENT));

    testContext.completeNow();
  }

  @ParameterizedTest
  @EnumSource(value = MetadataPrefix.class, names = { "MARC_XML", "DC" })
  void getOaiGetRecordVerbWithExistingIdentifier(MetadataPrefix metadataPrefix) throws JAXBException {
    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.EXISTING_IDENTIFIER;
    RequestSpecification request = createBaseRequest(GET_RECORD_PATH)
      .with()
      .pathParam(IDENTIFIER_PARAM, identifier)
      .with()
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());
    OAIPMH oaiPmhResponseWithExistingIdentifier = verify200WithXml(request, GET_RECORD);
    assertThat(oaiPmhResponseWithExistingIdentifier.getGetRecord(), is(notNullValue()));
    assertThat(oaiPmhResponseWithExistingIdentifier.getErrors(), is(empty()));
  }

  @ParameterizedTest
  @EnumSource(value = MetadataPrefix.class, names = { "MARC_XML", "DC" })
  void getOaiGetRecordVerbWithNonExistingIdentifier(MetadataPrefix metadataPrefix) throws
    JAXBException {
    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.NON_EXISTING_IDENTIFIER;
    RequestSpecification request = createBaseRequest(GET_RECORD_PATH)
      .with()
      .pathParam(IDENTIFIER_PARAM, identifier)
      .with()
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());

    OAIPMH oaipmh = verifyResponseWithErrors(request, GET_RECORD, 404, 1);
    assertThat(oaipmh.getGetRecord(), is(nullValue()));
    assertThat(oaipmh.getErrors().get(0).getCode(), equalTo(ID_DOES_NOT_EXIST));

  }


  @Test
  void getOaiMetadataFormats(VertxTestContext testContext) throws JAXBException {
    logger.info("=== Test Metadata Formats without identifier ===");

    OAIPMH oaiPmhResponseWithoutIdentifier = verify200WithXml(createBaseRequest(LIST_METADATA_FORMATS_PATH), LIST_METADATA_FORMATS);

    assertThat(oaiPmhResponseWithoutIdentifier.getListMetadataFormats(), is(notNullValue()));
    assertThat(oaiPmhResponseWithoutIdentifier.getErrors(), is(empty()));

    testContext.completeNow();
  }

  @Test
  void getOaiMetadataFormatsWithExistingIdentifier(VertxTestContext testContext) throws JAXBException {
    logger.info("=== Test Metadata Formats with existing identifier ===");

    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.EXISTING_IDENTIFIER;
    RequestSpecification request = createBaseRequest(LIST_METADATA_FORMATS_PATH)
      .with()
        .param(IDENTIFIER_PARAM, identifier);

    OAIPMH oaiPmhResponseWithExistingIdentifier = verify200WithXml(request, LIST_METADATA_FORMATS);

    assertThat(oaiPmhResponseWithExistingIdentifier.getListMetadataFormats(), is(notNullValue()));
    assertThat(oaiPmhResponseWithExistingIdentifier.getErrors(), is(empty()));

    testContext.completeNow();
  }

  @Test
  void getOaiMetadataFormatsWithNonExistingIdentifier(VertxTestContext testContext) throws JAXBException {
    logger.info("=== Test Metadata Formats with non-existing identifier ===");

    // Check that error message is returned
    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.NON_EXISTING_IDENTIFIER;
    RequestSpecification request = createBaseRequest(LIST_METADATA_FORMATS_PATH)
      .with()
        .param(IDENTIFIER_PARAM, identifier);

    OAIPMH oaipmh = verifyResponseWithErrors(request, LIST_METADATA_FORMATS, 404, 1);

    assertThat(oaipmh.getListMetadataFormats(), is(nullValue()));
    assertThat(oaipmh.getErrors().get(0).getCode(), equalTo(ID_DOES_NOT_EXIST));

    testContext.completeNow();
  }

  @Test
  void getOaiMetadataFormatsWithErrorFromStorage(VertxTestContext testContext) {
    logger.info("=== Test Metadata Formats with expected error from storage service ===");
    // Check that error message is returned
    RequestSpecification request = createBaseRequest(LIST_METADATA_FORMATS_PATH)
      .with()
        .param(IDENTIFIER_PARAM, IDENTIFIER_PREFIX + OkapiMockServer.ERROR_IDENTIFIER);

    verify500WithErrorMessage(request);

    testContext.completeNow();
  }

  @Test
  void getOaiMetadataFormatsWithInvalidIdentifier(VertxTestContext testContext) throws JAXBException {
    logger.info("=== Test Metadata Formats with invalid identifier format ===");

    // Check that error message is returned
    RequestSpecification request = createBaseRequest(LIST_METADATA_FORMATS_PATH)
      .with()
        .param(IDENTIFIER_PARAM, OkapiMockServer.INVALID_IDENTIFIER);

    OAIPMH oaipmh = verifyResponseWithErrors(request, LIST_METADATA_FORMATS, 422, 1);

    assertThat(oaipmh.getListMetadataFormats(), is(nullValue()));
    assertThat(oaipmh.getErrors().get(0).getCode(), equalTo(BAD_ARGUMENT));

    testContext.completeNow();
  }

  @Test
  void testSuccessfulGetOaiSets(VertxTestContext testContext) throws JAXBException {
    OAIPMH oaipmhFromString = verify200WithXml(createBaseRequest(LIST_SETS_PATH), LIST_SETS);

    assertThat(oaipmhFromString.getListSets(), is(notNullValue()));
    assertThat(oaipmhFromString.getListSets().getSets(), hasSize(equalTo(1)));
    assertThat(oaipmhFromString.getListSets().getSets().get(0).getSetSpec(), equalTo("all"));
    assertThat(oaipmhFromString.getListSets().getSets().get(0).getSetName(), equalTo("All records"));

    testContext.completeNow();
  }

  @Test
  void getOaiRepositoryInfoSuccess(VertxTestContext testContext) throws JAXBException {
    OAIPMH oaipmhFromString = verify200WithXml(createBaseRequest(IDENTIFY_PATH), IDENTIFY);

    assertThat(oaipmhFromString.getIdentify(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getBaseURL(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getAdminEmails(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getAdminEmails(), hasSize(equalTo(2)));
    assertThat(oaipmhFromString.getIdentify().getEarliestDatestamp(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getGranularity(), is(equalTo(GranularityType.YYYY_MM_DD_THH_MM_SS_Z)));
    assertThat(oaipmhFromString.getIdentify().getProtocolVersion(), is(equalTo("2.0")));
    assertThat(oaipmhFromString.getIdentify().getRepositoryName(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getCompressions(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getCompressions(), containsInAnyOrder(GZIP, DEFLATE));

    testContext.completeNow();
  }

  private RequestSpecification createBaseRequest(String basePath) {
    return RestAssured
      .given()
        .header(okapiUrlHeader)
        .header(tokenHeader)
        .header(tenantHeader)
        .basePath(basePath)
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

  private OAIPMH verify200WithXml(RequestSpecification request, VerbType verb) throws JAXBException {
    String response = verifyWithCodeWithXml(request, 200);

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh = ResponseHelper.getInstance().stringToOaiPmh(response);

    verifyBaseResponse(oaipmh, verb);

    return oaipmh;
  }

  private static String verifyWithCodeWithXml(RequestSpecification request, int code) {
    return request
      .when()
        .get()
      .then()
        .statusCode(code)
        .contentType(APPLICATION_XML_TYPE)
        .log().all()
        .extract()
          .body()
          .asString();
  }

  private static String verify500WithErrorMessage(RequestSpecification request) {
    String response = request
      .when()
        .get()
      .then()
        .statusCode(500)
        .contentType(ContentType.TEXT)
        .log().all()
        .extract()
          .body()
          .asString();

    assertThat(response, is(notNullValue()));

    return response;
  }

  private OAIPMH verifyResponseWithErrors(RequestSpecification request, VerbType verb, int statusCode, int errorsCount) throws JAXBException {
    String response = verifyWithCodeWithXml(request, statusCode);

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmhFromString = ResponseHelper.getInstance().stringToOaiPmh(response);

    verifyBaseResponse(oaipmhFromString, verb);
    assertThat(oaipmhFromString.getErrors(), is(notNullValue()));
    assertThat(oaipmhFromString.getErrors(), hasSize(errorsCount));

    return oaipmhFromString;
  }

  private void verifyHeader(HeaderType header) {
    assertThat(header.getIdentifier(), containsString(IDENTIFIER_PREFIX));
    assertThat(header.getSetSpecs(), hasSize(1));
    assertThat(header.getDatestamp(), is(notNullValue()));
  }
}
