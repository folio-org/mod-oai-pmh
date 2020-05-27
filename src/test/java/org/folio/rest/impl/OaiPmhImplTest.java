package org.folio.rest.impl;

import static io.restassured.RestAssured.given;
import static java.lang.String.format;
import static org.folio.oaipmh.Constants.DEFLATE;
import static org.folio.oaipmh.Constants.FROM_PARAM;
import static org.folio.oaipmh.Constants.GZIP;
import static org.folio.oaipmh.Constants.IDENTIFIER_PARAM;
import static org.folio.oaipmh.Constants.ISO_UTC_DATE_TIME;
import static org.folio.oaipmh.Constants.LIST_ILLEGAL_ARGUMENTS_ERROR;
import static org.folio.oaipmh.Constants.LIST_NO_REQUIRED_PARAM_ERROR;
import static org.folio.oaipmh.Constants.METADATA_PREFIX_PARAM;
import static org.folio.oaipmh.Constants.NEXT_RECORD_ID_PARAM;
import static org.folio.oaipmh.Constants.NO_RECORD_FOUND_ERROR;
import static org.folio.oaipmh.Constants.OFFSET_PARAM;
import static org.folio.oaipmh.Constants.REPOSITORY_ADMIN_EMAILS;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;
import static org.folio.oaipmh.Constants.REPOSITORY_ENABLE_OAI_SERVICE;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_NAME;
import static org.folio.oaipmh.Constants.REPOSITORY_STORAGE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.REPOSITORY_TIME_GRANULARITY;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_PARAM;
import static org.folio.oaipmh.Constants.SET_PARAM;
import static org.folio.oaipmh.Constants.SOURCE_RECORD_STORAGE;
import static org.folio.oaipmh.Constants.SUPPRESS_FROM_DISCOVERY_SUBFIELD_CODE;
import static org.folio.oaipmh.Constants.TOTAL_RECORDS_PARAM;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.Constants.VERB_PARAM;
import static org.folio.rest.impl.OkapiMockServer.DATE_FOR_INSTANCES_10;
import static org.folio.rest.impl.OkapiMockServer.INVALID_IDENTIFIER;
import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.folio.rest.impl.OkapiMockServer.PARTITIONABLE_RECORDS_DATE;
import static org.folio.rest.impl.OkapiMockServer.PARTITIONABLE_RECORDS_DATE_TIME;
import static org.folio.rest.impl.OkapiMockServer.THREE_INSTANCES_DATE;
import static org.folio.rest.impl.OkapiMockServer.THREE_INSTANCES_DATE_TIME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
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
import static org.openarchives.oai._2.VerbType.UNKNOWN;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.bind.JAXBElement;

import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.folio.oaipmh.Constants;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.ResponseConverter;
import org.folio.rest.RestVerticle;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.openarchives.oai._2.GranularityType;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.VerbType;
import org.openarchives.oai._2_0.oai_dc.Dc;
import org.openarchives.oai._2_0.oai_identifier.OaiIdentifier;
import org.purl.dc.elements._1.ElementType;

import gov.loc.marc21.slim.DataFieldType;
import gov.loc.marc21.slim.SubfieldatafieldType;
import io.restassured.RestAssured;
import io.restassured.config.DecoderConfig;
import io.restassured.config.DecoderConfig.ContentDecoder;
import io.restassured.config.RestAssuredConfig;
import io.restassured.http.ContentType;
import io.restassured.http.Header;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
@TestInstance(PER_CLASS)
class OaiPmhImplTest {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  // API paths
  private static final String ROOT_PATH = "/oai";
  private static final String RECORDS_PATH = ROOT_PATH + "/records";

  private static final int okapiPort = NetworkUtils.nextFreePort();
  private static final int mockPort = NetworkUtils.nextFreePort();

  private static final String XML_TYPE = "text/xml";
  private static final String TENANT = OAI_TEST_TENANT;
  private static final String IDENTIFIER_PREFIX = "oai:test.folio.org:" + TENANT + "/";
  private static final String[] ENCODINGS = {"GZIP", "DEFLATE", "IDENTITY"};
  private static final List<VerbType> LIST_VERBS = Arrays.asList(LIST_RECORDS, LIST_IDENTIFIERS);
  private final static String DATE_ONLY_GRANULARITY_PATTERN = "^\\d{4}-\\d{2}-\\d{2}$";
  private final static String DATE_TIME_GRANULARITY_PATTERN = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$";

  private static final String TEST_INSTANCE_ID = "00000000-0000-4000-a000-000000000000";
  private static final String TEST_INSTANCE_EXPECTED_VALUE_FOR_MARC21 = "0";
  private static final String TEST_INSTANCE_EXPECTED_VALUE_FOR_DC = "discovery not suppressed";

  private final Header tenantHeader = new Header("X-Okapi-Tenant", TENANT);
  private final Header tenantWithotConfigsHeader = new Header("X-Okapi-Tenant", "noConfigTenant");
  private final Header tokenHeader = new Header("X-Okapi-Token", "eyJhbGciOiJIUzI1NiJ9");
  private final Header okapiUrlHeader = new Header("X-Okapi-Url", "http://localhost:" + mockPort);

  private Predicate<DataFieldType> suppressedDiscoveryMarcFieldPredicate;
  private Predicate<JAXBElement<ElementType>> suppressedDiscoveryDcFieldPredicate;

  @BeforeAll
  void setUpOnce(Vertx vertx, VertxTestContext testContext) {
    resetSystemProperties();
    setStorageType();
    String moduleName = PomReader.INSTANCE.getModuleName()
                                          .replaceAll("_", "-");  // RMB normalizes the dash to underscore, fix back
    String moduleVersion = PomReader.INSTANCE.getVersion();
    String moduleId = moduleName + "-" + moduleVersion;
    getLogger().info("Test setup starting for " + moduleId);

    RestAssured.baseURI = "http://localhost:" + okapiPort;
    RestAssured.port = okapiPort;
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    JsonObject conf = new JsonObject()
      .put("http.port", okapiPort);

    getLogger().info(format("mod-oai-pmh test: Deploying %s with %s", RestVerticle.class.getName(), Json.encode(conf)));

    DeploymentOptions opt = new DeploymentOptions().setConfig(conf);
    vertx.deployVerticle(RestVerticle.class.getName(), opt, testContext.succeeding(id -> {
      getLogger().info("mod-oai-pmh Test: setup done. Using port " + okapiPort);
      // Once MockServer starts, it indicates to junit that process is finished by calling context.completeNow()
      new OkapiMockServer(vertx, mockPort).start(testContext);
    }));
    setupPredicates();
  }

  protected void setStorageType() {
    System.setProperty(REPOSITORY_STORAGE, SOURCE_RECORD_STORAGE);
  }

  protected Logger getLogger() {
    return logger;
  }

  private void resetSystemProperties() {
    System.clearProperty(REPOSITORY_BASE_URL);
    System.clearProperty(REPOSITORY_MAX_RECORDS_PER_RESPONSE);
    System.clearProperty(REPOSITORY_NAME);
    System.clearProperty(REPOSITORY_ADMIN_EMAILS);
    System.clearProperty(REPOSITORY_TIME_GRANULARITY);
    System.clearProperty(REPOSITORY_DELETED_RECORDS);
    System.clearProperty(REPOSITORY_STORAGE);
  }

  private void setupPredicates(){
    suppressedDiscoveryMarcFieldPredicate = (dataField) -> {
      List<SubfieldatafieldType> subfields = dataField.getSubfields();
      if (Objects.nonNull(subfields) && subfields.size() > 0) {
        return subfields.stream()
          .anyMatch(subfieldatafieldType -> {
            String value = subfieldatafieldType.getValue();
            return subfieldatafieldType.getCode()
              .equals(SUPPRESS_FROM_DISCOVERY_SUBFIELD_CODE) && value.equals("0") || value.equals("1");
          });
      }
      return false;
    };

    suppressedDiscoveryDcFieldPredicate = (jaxbElement) -> {
      String value = jaxbElement.getValue().getValue();
      return jaxbElement.getName().getLocalPart().equals("rights")
        && value.equals("discovery suppressed") || value.equals("discovery not suppressed");
    };
  }

  @BeforeEach
  void setUpBeforeEach() {
    // Set default decoderConfig
    RestAssured.config().decoderConfig(DecoderConfig.decoderConfig());
  }

  @Test
  void shouldRespondWithServiceUnavailableWhenGetVerbsAndEnableOaiSettingIsFalse() {
    System.setProperty(REPOSITORY_ENABLE_OAI_SERVICE, "false");
    RequestSpecification request = createBaseRequest(RECORDS_PATH);

    String stringOaipmh = verifyWithCodeWithXml(request, HttpStatus.SC_SERVICE_UNAVAILABLE);
    OAIPMH oaipmh = ResponseConverter.getInstance().stringToOaiPmh(stringOaipmh);
    verifyBaseResponse(oaipmh, UNKNOWN);
    System.setProperty(REPOSITORY_ENABLE_OAI_SERVICE, "true");
  }

  @ParameterizedTest
  @ValueSource(strings = { "GZIP", "DEFLATE", "IDENTITY" })
  void adminHealth(String encoding) {
    getLogger().debug(format("==== Starting adminHealth(%s) ====", encoding));

    // Simple GET request to see the module is running and we can talk to it.
    ValidatableResponse response = addAcceptEncodingHeader(encoding)
      .get("/admin/health")
      .then()
        .log().all()
        .statusCode(200);

    getLogger().debug(format("==== adminHealth(%s) successfully completed ====", encoding));
  }

  @ParameterizedTest
  @ValueSource(strings = { "GZIP", "DEFLATE", "IDENTITY" })
  void getOaiIdentifiersSuccess(String encoding) {
    getLogger().debug(format("==== Starting getOaiIdentifiersSuccess(%s) ====", encoding));

    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_IDENTIFIERS.value())
      .param(FROM_PARAM, DATE_FOR_INSTANCES_10)
      .param(METADATA_PREFIX_PARAM, MetadataPrefix.MARC21XML.getName());
    addAcceptEncodingHeader(request, encoding);

    OAIPMH oaipmh = verify200WithXml(request, LIST_IDENTIFIERS);

    verifyListResponse(oaipmh, LIST_IDENTIFIERS, 10);
    assertThat(oaipmh.getListIdentifiers().getResumptionToken(), is(nullValue()));

    getLogger().debug(format("==== getOaiIdentifiersSuccess(%s) successfully completed ====", encoding));
  }

  @ParameterizedTest
  @MethodSource("metadataPrefixAndEncodingProvider")
  void getOaiIdentifiersVerbOneRecordWithoutExternalIdsHolderField(MetadataPrefix metadataPrefix, String encoding) {
    getLogger().debug(format("==== Starting getOaiIdentifiersVerbOneRecordWithoutExternalIdsHolderField(%s, %s) ====", metadataPrefix.name(), encoding));

    String from = OkapiMockServer.DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOUT_EXTERNAL_IDS_HOLDER_FIELD;
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_IDENTIFIERS.value())
      .param(FROM_PARAM, from)
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());

    addAcceptEncodingHeader(request, encoding);

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh = verify200WithXml(request, LIST_IDENTIFIERS);

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix.getName()));
    assertThat(oaipmh.getRequest().getFrom(), equalTo(from));

    verifyListResponse(oaipmh, LIST_IDENTIFIERS, 2);

    getLogger().debug(format("==== getOaiIdentifiersVerbOneRecordWithoutExternalIdsHolderField(%s, %s) successfully completed ====", metadataPrefix.getName(), encoding));
  }

  @ParameterizedTest
  @MethodSource("metadataPrefixAndEncodingProvider")
  void getOaiIdentifiersWithDateRange(MetadataPrefix prefix, String encoding) {
    getLogger().debug(format("==== Starting getOaiIdentifiersWithDateRange(%s, %s) ====", prefix.name(), encoding));

    OAIPMH oaipmh = verifyOaiListVerbWithDateRange(LIST_IDENTIFIERS, prefix, encoding);

    verifyListResponse(oaipmh, LIST_IDENTIFIERS, 3);

    assertThat(oaipmh.getListIdentifiers().getResumptionToken(), is(nullValue()));

    oaipmh.getListIdentifiers()
          .getHeaders()
          .forEach(this::verifyHeader);

    getLogger().debug(format("==== getOaiIdentifiersWithDateRange(%s, %s) successfully completed ====", prefix.getName(), encoding));
  }

  @ParameterizedTest
  @MethodSource("metadataPrefixAndEncodingProvider")
  void getOaiRecordsWithDateTimeRange(MetadataPrefix prefix, String encoding) {
    getLogger().debug(format("==== Starting getOaiRecordsWithDateTimeRange(%s, %s) ====", prefix.name(), encoding));

    OAIPMH oaipmh = verifyOaiListVerbWithDateRange(LIST_RECORDS, prefix, encoding);

    verifyListResponse(oaipmh, LIST_RECORDS, 3);
    assertThat(oaipmh.getListRecords().getResumptionToken(), is(nullValue()));

    getLogger().debug(format("==== getOaiRecordsWithDateTimeRange(%s, %s) successfully completed ====", prefix.getName(), encoding));
  }

  private OAIPMH verifyOaiListVerbWithDateRange(VerbType verb, MetadataPrefix prefix, String encoding) {
    String metadataPrefix = prefix.getName();
    String from = THREE_INSTANCES_DATE_TIME;
    String until = "2018-12-19T02:52:08Z";
    String set = "all";

    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(FROM_PARAM, from)
      .param(UNTIL_PARAM, until)
      .param(METADATA_PREFIX_PARAM, metadataPrefix)
      .param(SET_PARAM, set);

    addAcceptEncodingHeader(request, encoding);

    OAIPMH oaipmh = verify200WithXml(request, verb);

    assertThat(oaipmh.getErrors(), is(empty()));

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix));
    assertThat(oaipmh.getRequest().getFrom(), equalTo(from));
    assertThat(oaipmh.getRequest().getUntil(), equalTo(until));
    assertThat(oaipmh.getRequest().getSet(), equalTo(set));

    return oaipmh;
  }

  @ParameterizedTest
  @EnumSource(MetadataPrefix.class)
  void getOaiRecordsWithDateRange(MetadataPrefix metadataPrefix) {
    getLogger().debug("==== Starting getOaiRecordsWithDateRange() ====");

    String from = THREE_INSTANCES_DATE;
    String until = "2018-12-20";

    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_RECORDS.value())
      .param(FROM_PARAM, from)
      .param(UNTIL_PARAM, until)
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());

    OAIPMH oaipmh = verify200WithXml(request, LIST_RECORDS);

    assertThat(oaipmh.getErrors(), is(empty()));

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix.getName()));
    assertThat(oaipmh.getRequest().getFrom(), equalTo(from));
    assertThat(oaipmh.getRequest().getUntil(), equalTo(until));

    verifyListResponse(oaipmh, LIST_RECORDS, 3);
    assertThat(oaipmh.getListRecords().getResumptionToken(), is(nullValue()));

    getLogger().debug("==== getOaiRecordsWithDateRange() successfully completed ====");
  }

  @Test
  void getOaiRecordsWithMixedDateAndDateTimeRange() {

    getLogger().debug("==== Starting getOaiRecordsWithMixedDateAndDateTimeRange() ====");

    String metadataPrefix = MetadataPrefix.MARC21XML.getName();
    String from = "2018-12-19";
    String until = "2018-12-19T02:52:08Z";

    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_RECORDS.value())
      .param(FROM_PARAM, from)
      .param(UNTIL_PARAM, until)
      .param(METADATA_PREFIX_PARAM, metadataPrefix);

    OAIPMH oaipmh = verifyResponseWithErrors(request, LIST_RECORDS, 400, 1);

    assertThat(oaipmh.getErrors().get(0).getCode(), equalTo(BAD_ARGUMENT));

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix));
    assertThat(oaipmh.getRequest().getFrom(), equalTo(from));
    assertThat(oaipmh.getRequest().getUntil(), equalTo(until));

    getLogger().debug("==== getOaiRecordsWithMixedDateAndDateTimeRange() successfully completed ====");
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS"})
  void getOaiListVerbWithoutParams(VerbType verb) {
    RequestSpecification request = createBaseRequest(RECORDS_PATH).with()
      .param(VERB_PARAM, verb.value());
    List<OAIPMHerrorType> errors = verifyResponseWithErrors(request, verb, 400, 1).getErrors();
    OAIPMHerrorType error = errors.get(0);
    assertThat(error.getCode(), equalTo(BAD_ARGUMENT));
    assertThat(error.getValue(), equalTo(LIST_NO_REQUIRED_PARAM_ERROR));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiListVerbWithWrongMetadataPrefix(VerbType verb) {
    String metadataPrefix = "abc";
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(METADATA_PREFIX_PARAM, metadataPrefix);

    OAIPMH oaipmh = verifyResponseWithErrors(request, verb, 422, 1);

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix));

    List<OAIPMHerrorType> errors = oaipmh.getErrors();
    assertThat(errors.get(0).getCode(), equalTo(CANNOT_DISSEMINATE_FORMAT));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiListVerbResumptionFlowStarted(VerbType verb) {
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(FROM_PARAM, PARTITIONABLE_RECORDS_DATE_TIME)
      .param(METADATA_PREFIX_PARAM, "oai_dc")
      .param(SET_PARAM, "all");


    OAIPMH oaipmh = verify200WithXml(request, verb);

    verifyListResponse(oaipmh, verb, 10);

    ResumptionTokenType resumptionToken = getResumptionToken(oaipmh, verb);

    assertThat(resumptionToken, is(notNullValue()));
    assertThat(resumptionToken.getCompleteListSize(), is(equalTo(BigInteger.valueOf(100))));
    assertThat(resumptionToken.getCursor(), is(equalTo(BigInteger.ZERO)));
    assertThat(resumptionToken.getExpirationDate(), is(nullValue()));

    String resumptionTokenValue =
      new String(Base64.getUrlDecoder().decode(resumptionToken.getValue()), StandardCharsets.UTF_8);
    List<NameValuePair> params = URLEncodedUtils.parse(resumptionTokenValue, StandardCharsets.UTF_8);
    assertThat(params, is(hasSize(7)));

    assertThat(getParamValue(params, METADATA_PREFIX_PARAM), is(equalTo("oai_dc")));
    assertThat(getParamValue(params, FROM_PARAM), is(equalTo(PARTITIONABLE_RECORDS_DATE_TIME)));
    assertThat(getParamValue(params, UNTIL_PARAM), is((notNullValue())));
    assertThat(getParamValue(params, SET_PARAM), is(equalTo("all")));
    assertThat(getParamValue(params, OFFSET_PARAM), is(equalTo("10")));
    assertThat(getParamValue(params, TOTAL_RECORDS_PARAM), is(equalTo("100")));
    assertThat(getParamValue(params, NEXT_RECORD_ID_PARAM), is(equalTo("6506b79b-7702-48b2-9774-a1c538fdd34e")));
  }

  @ParameterizedTest
  @MethodSource("metadataPrefixAndVerbProvider")
  void getOaiListVerbResumptionFlowStartedWithFromParamHasDateAndTimeGranularity(MetadataPrefix prefix, VerbType verb) {
    String timeGranularity = System.getProperty(REPOSITORY_TIME_GRANULARITY);
    System.setProperty(REPOSITORY_TIME_GRANULARITY, GranularityType.YYYY_MM_DD_THH_MM_SS_Z.value());

    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(FROM_PARAM, PARTITIONABLE_RECORDS_DATE_TIME)
      .param(METADATA_PREFIX_PARAM, prefix.getName())
      .param(SET_PARAM, "all");

    OAIPMH oaipmh = verify200WithXml(request, verb);

    ResumptionTokenType resumptionToken = getResumptionToken(oaipmh, verb);

    //rollback changes of system properties as they were before test
    System.setProperty(REPOSITORY_TIME_GRANULARITY, timeGranularity);
    assertThat(resumptionToken, is(notNullValue()));

    String resumptionTokenValue = new String(Base64.getUrlDecoder().decode(resumptionToken.getValue()), StandardCharsets.UTF_8);
    List<NameValuePair> params = URLEncodedUtils.parse(resumptionTokenValue, StandardCharsets.UTF_8);
    assertThat(params, is(hasSize(7)));
    assertTrue(getParamValue(params, UNTIL_PARAM).matches(DATE_TIME_GRANULARITY_PATTERN));
  }

  @ParameterizedTest
  @MethodSource("metadataPrefixAndVerbProvider")
  void getOaiListVerbResumptionFlowStartedWithFromParamHasDateOnlyGranularity(MetadataPrefix prefix, VerbType verb) {
    String timeGranularity = System.getProperty(REPOSITORY_TIME_GRANULARITY);
    System.setProperty(REPOSITORY_TIME_GRANULARITY, GranularityType.YYYY_MM_DD_THH_MM_SS_Z.value());

    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(FROM_PARAM, PARTITIONABLE_RECORDS_DATE)
      .param(METADATA_PREFIX_PARAM, prefix.getName())
      .param(SET_PARAM, "all");

    OAIPMH oaipmh = verify200WithXml(request, verb);

    ResumptionTokenType resumptionToken = getResumptionToken(oaipmh, verb);

    //rollback changes of system properties as they were before test
    System.setProperty(REPOSITORY_TIME_GRANULARITY, timeGranularity);
    assertThat(resumptionToken, is(notNullValue()));

    String resumptionTokenValue = new String(Base64.getUrlDecoder().decode(resumptionToken.getValue()), StandardCharsets.UTF_8);
    List<NameValuePair> params = URLEncodedUtils.parse(resumptionTokenValue, StandardCharsets.UTF_8);
    assertThat(params, is(hasSize(7)));
    assertTrue(getParamValue(params, UNTIL_PARAM).matches(DATE_ONLY_GRANULARITY_PATTERN));
  }

  @ParameterizedTest
  @MethodSource("metadataPrefixAndVerbProvider")
  void getOaiListVerbResumptionFlowStartedWithoutFromParamAndGranularitySettingIsFull(MetadataPrefix prefix, VerbType verb) {
    String timeGranularity = System.getProperty(REPOSITORY_TIME_GRANULARITY);
    System.setProperty(REPOSITORY_TIME_GRANULARITY, GranularityType.YYYY_MM_DD_THH_MM_SS_Z.value());

    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(METADATA_PREFIX_PARAM, prefix.getName())
      .param(SET_PARAM, "all");

    OAIPMH oaipmh = verify200WithXml(request, verb);

    ResumptionTokenType resumptionToken = getResumptionToken(oaipmh, verb);

    //rollback changes of system properties as they were before test
    System.setProperty(REPOSITORY_TIME_GRANULARITY, timeGranularity);
    assertThat(resumptionToken, is(notNullValue()));

    String resumptionTokenValue = new String(Base64.getUrlDecoder().decode(resumptionToken.getValue()), StandardCharsets.UTF_8);
    List<NameValuePair> params = URLEncodedUtils.parse(resumptionTokenValue, StandardCharsets.UTF_8);
    assertThat(params, is(hasSize(6)));
    assertTrue(getParamValue(params, UNTIL_PARAM).matches(DATE_TIME_GRANULARITY_PATTERN));
  }

  @ParameterizedTest
  @MethodSource("metadataPrefixAndVerbProvider")
  void getOaiListVerbResumptionFlowStartedWithFromParamHasDateOnlyGranularityAndGranularitySettingIsDateOnly(MetadataPrefix prefix, VerbType verb) {
    String timeGranularity = System.getProperty(REPOSITORY_TIME_GRANULARITY);
    System.setProperty(REPOSITORY_TIME_GRANULARITY, GranularityType.YYYY_MM_DD.value());

    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(FROM_PARAM, PARTITIONABLE_RECORDS_DATE)
      .param(METADATA_PREFIX_PARAM, prefix.getName())
      .param(SET_PARAM, "all");

    OAIPMH oaipmh = verify200WithXml(request, verb);

    ResumptionTokenType resumptionToken = getResumptionToken(oaipmh, verb);

    //rollback changes of system properties as they were before test
    System.setProperty(REPOSITORY_TIME_GRANULARITY, timeGranularity);
    assertThat(resumptionToken, is(notNullValue()));

    String resumptionTokenValue = new String(Base64.getUrlDecoder().decode(resumptionToken.getValue()), StandardCharsets.UTF_8);
    List<NameValuePair> params = URLEncodedUtils.parse(resumptionTokenValue, StandardCharsets.UTF_8);
    assertThat(params, is(hasSize(7)));
    assertTrue(getParamValue(params, UNTIL_PARAM).matches(DATE_ONLY_GRANULARITY_PATTERN));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiListVerbWithResumptionTokenSuccessful(VerbType verb) {
    // base64 encoded string:
    // metadataPrefix=oai_dc&from=2003-01-01T00:00:00Z&until=2003-10-01T00:00:00Z&set=all
    // &offset=0&totalRecords=100&nextRecordId=04489a01-f3cd-4f9e-9be4-d9c198703f46
    String resumptionToken = "bWV0YWRhdGFQcmVmaXg9b2FpX2RjJmZyb209MjAwMy0wMS0wMVQwMDowMDowMFomdW50aWw9MjAwMy" +
      "0xMC0wMVQwMDowMDowMFomc2V0PWFsbCZvZmZzZXQ9MCZ0b3RhbFJlY29yZHM9MTAwJm5leHRSZWNvcmRJZD0wNDQ4OWEwMS1mM2N" +
      "kLTRmOWUtOWJlNC1kOWMxOTg3MDNmNDY";
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(RESUMPTION_TOKEN_PARAM, resumptionToken);

    OAIPMH oaipmh = verify200WithXml(request, verb);

    verifyListResponse(oaipmh, verb, 10);

    ResumptionTokenType actualResumptionToken = getResumptionToken(oaipmh, verb);
    assertThat(actualResumptionToken, is(notNullValue()));
    assertThat(actualResumptionToken.getValue(), is(notNullValue()));
    String actualValue =
      new String(Base64.getDecoder().decode(actualResumptionToken.getValue()), StandardCharsets.UTF_8);
    String expectedValue = actualValue.replaceAll("offset=\\d+", "offset=10");
    assertThat(actualValue, equalTo(expectedValue));
    assertThat(actualResumptionToken.getCompleteListSize(), is(equalTo(BigInteger.valueOf(100))));
    assertThat(actualResumptionToken.getCursor(), is(equalTo(BigInteger.ZERO)));
    assertThat(actualResumptionToken.getExpirationDate(), is(nullValue()));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiRecordsWithoutFromAndWithMetadataPrefixMarc21AndResumptionToken(VerbType verb) {
    String set = "all";
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(METADATA_PREFIX_PARAM, MetadataPrefix.MARC21XML.getName())
      .param(SET_PARAM, set);

    OAIPMH oaipmh = verify200WithXml(request, verb);
    verifyListResponse(oaipmh, verb, 9);
    ResumptionTokenType actualResumptionToken = getResumptionToken(oaipmh, verb);
    assertThat(actualResumptionToken, is(notNullValue()));
    assertThat(actualResumptionToken.getValue(), is(notNullValue()));

    RequestSpecification requestWithResumptionToken = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(RESUMPTION_TOKEN_PARAM, actualResumptionToken.getValue());

    OAIPMH oai = verify200WithXml(requestWithResumptionToken, verb);
    ResumptionTokenType nextResumptionToken = getResumptionToken(oai, verb);
    assertThat(nextResumptionToken, is(notNullValue()));
    assertThat(nextResumptionToken.getValue(),is(""));
    assertThat(nextResumptionToken.getCompleteListSize(), is(equalTo(BigInteger.valueOf(11))));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiRecordsWithoutFromAndWithMetadataPrefixDCAndResumptionToken(VerbType verb) {
    String set = "all";
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(METADATA_PREFIX_PARAM, MetadataPrefix.DC.getName())
      .param(SET_PARAM, set);

    OAIPMH oaipmh = verify200WithXml(request, verb);
    verifyListResponse(oaipmh, verb, 9);
    ResumptionTokenType actualResumptionToken = getResumptionToken(oaipmh, verb);
    assertThat(actualResumptionToken, is(notNullValue()));
    assertThat(actualResumptionToken.getValue(), is(notNullValue()));

    RequestSpecification requestWithResumptionToken = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(RESUMPTION_TOKEN_PARAM, actualResumptionToken.getValue());

    OAIPMH oai = verify200WithXml(requestWithResumptionToken, verb);
    ResumptionTokenType nextResumptionToken = getResumptionToken(oai, verb);
    assertThat(nextResumptionToken, is(notNullValue()));
    assertThat(nextResumptionToken.getValue(),is(""));
    assertThat(nextResumptionToken.getCompleteListSize(), is(equalTo(BigInteger.valueOf(11))));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiRecordsWithFromAndMetadataPrefixMarc21AndResumptionToken(VerbType verb) {
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(METADATA_PREFIX_PARAM, MetadataPrefix.MARC21XML.getName())
      .param(FROM_PARAM, PARTITIONABLE_RECORDS_DATE_TIME);

    OAIPMH oaipmh = verify200WithXml(request, verb);
    verifyListResponse(oaipmh, verb, 10);
    ResumptionTokenType actualResumptionToken = getResumptionToken(oaipmh, verb);
    assertThat(actualResumptionToken, is(notNullValue()));
    assertThat(actualResumptionToken.getValue(), is(notNullValue()));

    RequestSpecification requestWithResumptionToken = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(RESUMPTION_TOKEN_PARAM, actualResumptionToken.getValue());

    OAIPMH oai = verify200WithXml(requestWithResumptionToken, verb);
    ResumptionTokenType nextResumptionToken = getResumptionToken(oai, verb);
    assertThat(nextResumptionToken, is(notNullValue()));
    assertThat(nextResumptionToken.getValue(), is(notNullValue()));
    assertThat(nextResumptionToken.getCompleteListSize(), is(equalTo(BigInteger.valueOf(100))));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiRecordsWithFromAndMetadataPrefixDCAndResumptionToken(VerbType verb) {
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(METADATA_PREFIX_PARAM, MetadataPrefix.DC.getName())
      .param(FROM_PARAM, PARTITIONABLE_RECORDS_DATE_TIME);

    OAIPMH oaipmh = verify200WithXml(request, verb);
    verifyListResponse(oaipmh, verb, 10);
    ResumptionTokenType actualResumptionToken = getResumptionToken(oaipmh, verb);
    assertThat(actualResumptionToken, is(notNullValue()));
    assertThat(actualResumptionToken.getValue(), is(notNullValue()));

    RequestSpecification requestWithResumptionToken = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(RESUMPTION_TOKEN_PARAM, actualResumptionToken.getValue());

    OAIPMH oai = verify200WithXml(requestWithResumptionToken, verb);
    ResumptionTokenType nextResumptionToken = getResumptionToken(oai, verb);
    assertThat(nextResumptionToken, is(notNullValue()));
    assertThat(nextResumptionToken.getValue(), is(notNullValue()));
    assertThat(nextResumptionToken.getCompleteListSize(), is(equalTo(BigInteger.valueOf(100))));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiRecordsWithFromAndUntilAndMetadataPrefixMarc21AndResumptionToken(VerbType verb) {
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(METADATA_PREFIX_PARAM, MetadataPrefix.MARC21XML.getName())
      .param(FROM_PARAM, PARTITIONABLE_RECORDS_DATE_TIME)
      .param(UNTIL_PARAM, LocalDateTime.now(ZoneOffset.UTC).format(ISO_UTC_DATE_TIME));

    OAIPMH oaipmh = verify200WithXml(request, verb);
    verifyListResponse(oaipmh, verb, 10);
    ResumptionTokenType actualResumptionToken = getResumptionToken(oaipmh, verb);
    assertThat(actualResumptionToken, is(notNullValue()));
    assertThat(actualResumptionToken.getValue(), is(notNullValue()));

    RequestSpecification requestWithResumptionToken = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(RESUMPTION_TOKEN_PARAM, actualResumptionToken.getValue());

    OAIPMH oai = verify200WithXml(requestWithResumptionToken, verb);
    ResumptionTokenType nextResumptionToken = getResumptionToken(oai, verb);
    assertThat(nextResumptionToken, is(notNullValue()));
    assertThat(nextResumptionToken.getValue(), is(notNullValue()));
    assertThat(nextResumptionToken.getCompleteListSize(), is(equalTo(BigInteger.valueOf(100))));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiRecordsWithFromAndUntilAndMetadataPrefixDCAndResumptionToken(VerbType verb) {
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(METADATA_PREFIX_PARAM, MetadataPrefix.DC.getName())
      .param(FROM_PARAM, PARTITIONABLE_RECORDS_DATE_TIME)
      .param(UNTIL_PARAM, LocalDateTime.now(ZoneOffset.UTC).format(ISO_UTC_DATE_TIME));

    OAIPMH oaipmh = verify200WithXml(request, verb);
    verifyListResponse(oaipmh, verb, 10);
    ResumptionTokenType actualResumptionToken = getResumptionToken(oaipmh, verb);
    assertThat(actualResumptionToken, is(notNullValue()));
    assertThat(actualResumptionToken.getValue(), is(notNullValue()));

    RequestSpecification requestWithResumptionToken = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(RESUMPTION_TOKEN_PARAM, actualResumptionToken.getValue());

    OAIPMH oai = verify200WithXml(requestWithResumptionToken, verb);
    ResumptionTokenType nextResumptionToken = getResumptionToken(oai, verb);
    assertThat(nextResumptionToken, is(notNullValue()));
    assertThat(nextResumptionToken.getValue(), is(notNullValue()));
    assertThat(nextResumptionToken.getCompleteListSize(), is(equalTo(BigInteger.valueOf(100))));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiRecordsWithUntilAndMetadataPrefixMarc21AndResumptionToken(VerbType verb) {
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(METADATA_PREFIX_PARAM, MetadataPrefix.MARC21XML.getName())
      .param(UNTIL_PARAM, LocalDateTime.now(ZoneOffset.UTC).format(ISO_UTC_DATE_TIME));

    OAIPMH oaipmh = verify200WithXml(request, verb);
    verifyListResponse(oaipmh, verb, 9);
    ResumptionTokenType actualResumptionToken = getResumptionToken(oaipmh, verb);
    assertThat(actualResumptionToken, is(notNullValue()));
    assertThat(actualResumptionToken.getValue(), is(notNullValue()));

    RequestSpecification requestWithResumptionToken = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(RESUMPTION_TOKEN_PARAM, actualResumptionToken.getValue());

    OAIPMH oai = verify200WithXml(requestWithResumptionToken, verb);
    ResumptionTokenType nextResumptionToken = getResumptionToken(oai, verb);
    assertThat(nextResumptionToken, is(notNullValue()));
    assertThat(nextResumptionToken.getValue(), is(notNullValue()));
    assertThat(nextResumptionToken.getCompleteListSize(), is(equalTo(BigInteger.valueOf(11))));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiRecordsWithUntilAndMetadataPrefixDCAndResumptionToken(VerbType verb) {
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(METADATA_PREFIX_PARAM, MetadataPrefix.DC.getName())
      .param(UNTIL_PARAM, LocalDateTime.now(ZoneOffset.UTC).format(ISO_UTC_DATE_TIME));

    OAIPMH oaipmh = verify200WithXml(request, verb);
    verifyListResponse(oaipmh, verb, 9);
    ResumptionTokenType actualResumptionToken = getResumptionToken(oaipmh, verb);
    assertThat(actualResumptionToken, is(notNullValue()));
    assertThat(actualResumptionToken.getValue(), is(notNullValue()));

    RequestSpecification requestWithResumptionToken = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(RESUMPTION_TOKEN_PARAM, actualResumptionToken.getValue());

    OAIPMH oai = verify200WithXml(requestWithResumptionToken, verb);
    ResumptionTokenType nextResumptionToken = getResumptionToken(oai, verb);
    assertThat(nextResumptionToken, is(notNullValue()));
    assertThat(nextResumptionToken.getValue(), is(notNullValue()));
    assertThat(nextResumptionToken.getCompleteListSize(), is(equalTo(BigInteger.valueOf(11))));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiListVerbWithBadResumptionToken(VerbType verb) {
    // base64 encoded string:
    // metadataPrefix=oai_dc&from=2003-01-01T00:00:00Z&until=2003-10-01T00:00:00Z
    // &set=all&offset=0&totalRecords=101&nextRecordId=6506b79b-7702-48b2-9774-a1c538fdd34e
    String resumptionToken = "bWV0YWRhdGFQcmVmaXg9b2FpX2RjJmZyb209MjAwMy0wMS0wMVQwMDowMDowMFomdW50aWw9M" +
      "jAwMy0xMC0wMVQwMDowMDowMFomc2V0PWFsbCZvZmZzZXQ9MCZ0b3RhbFJlY29yZHM9MTAxJm5leHRSZWNvcmRJZD02NTA2Y" +
      "jc5Yi03NzAyLTQ4YjItOTc3NC1hMWM1MzhmZGQzNGU";
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(RESUMPTION_TOKEN_PARAM, resumptionToken);

    OAIPMH oaipmh = verifyResponseWithErrors(request, verb, 400, 1);
    assertThat(oaipmh.getErrors(), is(hasSize(1)));
    assertThat(oaipmh.getErrors().get(0).getCode(), is(equalTo(BAD_RESUMPTION_TOKEN)));
    assertThat(oaipmh.getRequest().getResumptionToken(), equalTo(resumptionToken));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiListVerbWithResumptionTokenAndMetadataPrefix(VerbType verb) {
    String resumptionToken = "abc";
    String metadataPrefix = "oai_dc";
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(METADATA_PREFIX_PARAM, metadataPrefix)
      .param(RESUMPTION_TOKEN_PARAM, resumptionToken);

    OAIPMH oaipmh = verifyResponseWithErrors(request, verb, 400, 1);

    assertThat(oaipmh.getRequest().getResumptionToken(), equalTo(resumptionToken));
    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix));

    List<OAIPMHerrorType> errors = oaipmh.getErrors();
    assertThat(oaipmh.getErrors().get(0).getCode(), is(equalTo(BAD_ARGUMENT)));

    Optional<String> badArgMsg = errors.stream().filter(error -> error.getCode() == BAD_ARGUMENT).map(OAIPMHerrorType::getValue).findAny();
    badArgMsg.ifPresent(msg -> assertThat(msg, equalTo(format(LIST_ILLEGAL_ARGUMENTS_ERROR, verb.name()))));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiListVerbWithWrongSet(VerbType verb) {
    String metadataPrefix = MetadataPrefix.MARC21XML.getName();
    String set = "single";

    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
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
  void getOaiListVerbWithWrongDatesAndWrongSet(VerbType verb) {
    String metadataPrefix = MetadataPrefix.MARC21XML.getName();
    String from = "2018-09-19T02:52:08.873";
    String until = "2018-10-20T02:03:04.567";
    String set = "single";

    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
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
  void getOaiListVerbWithInvalidDateRange(VerbType verb) {
    String metadataPrefix = MetadataPrefix.MARC21XML.getName();
    String from = "2018-12-19T02:52:08Z";
    String until = "2018-10-20T02:03:04Z";

    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
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
  void getOaiListVerbWithNoRecordsFoundFromStorage(VerbType verb) {
    String metadataPrefix = MetadataPrefix.DC.getName();
    String from = OkapiMockServer.NO_RECORDS_DATE;
    String set = "all";

    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
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
  @MethodSource("metadataPrefixAndEncodingProvider")
  void getOaiListRecordsVerbWithOneNotFoundRecordFromStorage(MetadataPrefix metadataPrefix, String encoding) {
    getLogger().debug(format("==== Starting getOaiListRecordsVerbWithOneNotFoundRecordFromStorage(%s, %s) ====", metadataPrefix.name(), encoding));

    String from = OkapiMockServer.DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOT_RECORD;
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_RECORDS.value())
      .param(FROM_PARAM, from)
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());

    addAcceptEncodingHeader(request, encoding);

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh = verify200WithXml(request, LIST_RECORDS);

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix.getName()));
    assertThat(oaipmh.getRequest().getFrom(), equalTo(from));

    verifyListResponse(oaipmh, LIST_RECORDS, 3);

    getLogger().debug(format("==== getOaiListRecordsVerbWithOneNotFoundRecordFromStorage(%s, %s) successfully completed ====", metadataPrefix.getName(), encoding));
  }

  @ParameterizedTest
  @MethodSource("metadataPrefixAndEncodingProvider")
  void getOaiListRecordsVerbWithOneWithoutExternalIdsHolderField(MetadataPrefix metadataPrefix, String encoding) {
    getLogger().debug(format("==== Starting getOaiListRecordsVerbWithOneWithoutExternalIdsHolderField(%s, %s) ====", metadataPrefix.name(), encoding));

    String from = OkapiMockServer.DATE_FOR_FOUR_INSTANCES_BUT_ONE_WITHOUT_EXTERNAL_IDS_HOLDER_FIELD;
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_RECORDS.value())
      .param(FROM_PARAM, from)
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());

    addAcceptEncodingHeader(request, encoding);

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh = verify200WithXml(request, LIST_RECORDS);

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix.getName()));
    assertThat(oaipmh.getRequest().getFrom(), equalTo(from));

    verifyListResponse(oaipmh, LIST_RECORDS, 2);

    getLogger().debug(format("==== getOaiListRecordsVerbWithOneWithoutExternalIdsHolderField(%s, %s) successfully completed ====", metadataPrefix.getName(), encoding));
  }

  @ParameterizedTest
  @MethodSource("metadataPrefixAndEncodingProvider")
  void getOaiListRecordsVerbAndSuppressDiscoveryProcessingSettingHasFalseValue(MetadataPrefix metadataPrefix, String encoding) {
    getLogger().debug(format("==== Starting getOaiListRecordsVerbWithOneWithoutExternalIdsHolderField(%s, %s) ====", metadataPrefix.name(), encoding));

    String repositorySuppressDiscovery = System.getProperty(REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
    System.setProperty(REPOSITORY_SUPPRESSED_RECORDS_PROCESSING, "false");

    String from = OkapiMockServer.THREE_INSTANCES_DATE;
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_RECORDS.value())
      .param(FROM_PARAM, from)
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());

    addAcceptEncodingHeader(request, encoding);

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh = verify200WithXml(request, LIST_RECORDS);

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix.getName()));
    assertThat(oaipmh.getRequest().getFrom(), equalTo(from));

    verifyListResponse(oaipmh, LIST_RECORDS, 3);
    verifySuppressedDiscoveryFieldPresence(oaipmh, LIST_RECORDS, metadataPrefix, false);
    System.setProperty(REPOSITORY_SUPPRESSED_RECORDS_PROCESSING, repositorySuppressDiscovery);
    getLogger().debug(format("==== getOaiListRecordsVerbWithOneWithoutExternalIdsHolderField(%s, %s) successfully completed ====", metadataPrefix.getName(), encoding));
  }

  @ParameterizedTest
  @MethodSource("metadataPrefixAndEncodingProvider")
  void getOaiListRecordsVerbAndSuppressDiscoveryProcessingSettingHasTrueValue(MetadataPrefix metadataPrefix, String encoding) {
    getLogger().debug(format("==== Starting getOaiListRecordsVerbWithOneWithoutExternalIdsHolderField(%s, %s) ====", metadataPrefix.name(), encoding));

    String repositorySuppressDiscovery = System.getProperty(REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
    System.setProperty(REPOSITORY_SUPPRESSED_RECORDS_PROCESSING, "true");

    String from = OkapiMockServer.THREE_INSTANCES_DATE;
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_RECORDS.value())
      .param(FROM_PARAM, from)
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());

    addAcceptEncodingHeader(request, encoding);

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh = verify200WithXml(request, LIST_RECORDS);

    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix.getName()));
    assertThat(oaipmh.getRequest().getFrom(), equalTo(from));

    verifyListResponse(oaipmh, LIST_RECORDS, 3);
    verifySuppressedDiscoveryFieldPresence(oaipmh, LIST_RECORDS, metadataPrefix, true);
    verifySuppressDiscoveryFieldHasCorrectValue(oaipmh, LIST_RECORDS, metadataPrefix);

    System.setProperty(REPOSITORY_SUPPRESSED_RECORDS_PROCESSING, repositorySuppressDiscovery);
    getLogger().debug(format("==== getOaiListRecordsVerbWithOneWithoutExternalIdsHolderField(%s, %s) successfully completed ====", metadataPrefix.getName(), encoding));
  }

  @ParameterizedTest
  @EnumSource(MetadataPrefix.class)
  void getOaiListRecordsVerbWithErrorFromRecordStorage(MetadataPrefix metadataPrefix) {
    getLogger().debug(format("==== Starting getOaiListRecordsVerbWithErrorFromRecordStorage(%s) ====", metadataPrefix.getName()));

    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_RECORDS.value())
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName())
      .param(UNTIL_PARAM, OkapiMockServer.RECORD_STORAGE_INTERNAL_SERVER_ERROR_UNTIL_DATE);

    verify500WithErrorMessage(request);

    getLogger().debug(format("==== getOaiListRecordsVerbWithErrorFromRecordStorage(%s) successfully completed ====", metadataPrefix.getName()));
  }

  @ParameterizedTest
  @EnumSource(value = VerbType.class, names = { "LIST_IDENTIFIERS", "LIST_RECORDS" })
  void getOaiIdentifiersWithErrorFromStorage(VerbType verb) {
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, verb.value())
      .param(METADATA_PREFIX_PARAM, MetadataPrefix.DC.getName())
      .param(UNTIL_PARAM, OkapiMockServer.ERROR_UNTIL_DATE);

    verify500WithErrorMessage(request);
  }

  @ParameterizedTest
  @EnumSource(MetadataPrefix.class)
  void getOaiRecordByIdInvalidIdentifier(MetadataPrefix metadataPrefix) {
    RequestSpecification requestSpecification = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, GET_RECORD.value())
      .param(IDENTIFIER_PARAM, INVALID_IDENTIFIER)
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());
    String response = verifyWithCodeWithXml(requestSpecification, 400);

    // Check that error message is returned
    assertThat(response, is(notNullValue()));

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh = ResponseConverter.getInstance().stringToOaiPmh(response);
    verifyBaseResponse(oaipmh, GET_RECORD);
    assertThat(oaipmh.getGetRecord(), is(nullValue()));
    assertThat(oaipmh.getErrors().get(0).getCode(), equalTo(BAD_ARGUMENT));
  }

  @ParameterizedTest
  @EnumSource(MetadataPrefix.class)
  void getOaiListRecordsVerbWithOneInstanceButNotFoundRecordFromStorage(MetadataPrefix metadataPrefix) {
    String from = OkapiMockServer.DATE_FOR_ONE_INSTANCE_BUT_WITHOT_RECORD;
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_RECORDS.value())
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
  @EnumSource(MetadataPrefix.class)
  void getOaiGetRecordVerbWithOneInstanceButNotFoundRecordFromStorage(MetadataPrefix metadataPrefix) {
    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.NOT_FOUND_RECORD_INSTANCE_ID;
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, GET_RECORD.value())
      .param(IDENTIFIER_PARAM, identifier)
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());

    OAIPMH oaipmh = verifyResponseWithErrors(request, GET_RECORD, 404, 1);

    // The dates are of invalid format so they are not present in request
    assertThat(oaipmh.getRequest().getMetadataPrefix(), equalTo(metadataPrefix.getName()));
    assertThat(oaipmh.getRequest().getIdentifier(), equalTo(identifier));

    OAIPMHerrorType error = oaipmh.getErrors().get(0);
    assertThat(error.getCode(), equalTo(ID_DOES_NOT_EXIST));
  }

  @Test
  void getOaiGetRecordVerbWithWrongMetadataPrefix() {
    String metadataPrefix = "mark_xml";
    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.EXISTING_IDENTIFIER;
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, GET_RECORD.value())
      .param(IDENTIFIER_PARAM, identifier)
      .param(METADATA_PREFIX_PARAM, metadataPrefix);
    OAIPMH oaipmh = verifyResponseWithErrors(request, GET_RECORD, 422, 1);
    assertThat(oaipmh.getGetRecord(), is(nullValue()));
    assertThat(oaipmh.getErrors().get(0).getCode(), equalTo(CANNOT_DISSEMINATE_FORMAT));
  }

  @Test
  void getOaiGetRecordVerbWithoutMetadataPrefix(VertxTestContext testContext) {
    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.EXISTING_IDENTIFIER;
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, GET_RECORD.value())
      .param(IDENTIFIER_PARAM, identifier);
    OAIPMH oaipmh = verifyResponseWithErrors(request, GET_RECORD, 400, 1);
    assertThat(oaipmh.getGetRecord(), is(nullValue()));
    assertThat(oaipmh.getErrors().get(0).getCode(), equalTo(BAD_ARGUMENT));

    testContext.completeNow();
  }

  @ParameterizedTest
  @EnumSource(MetadataPrefix.class)
  void getOaiGetRecordVerbWithExistingIdentifier(MetadataPrefix metadataPrefix) {
    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.EXISTING_IDENTIFIER;
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, GET_RECORD.value())
      .param(IDENTIFIER_PARAM, identifier)
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());
    OAIPMH oaiPmhResponseWithExistingIdentifier = verify200WithXml(request, GET_RECORD);
    HeaderType recordHeader = oaiPmhResponseWithExistingIdentifier.getGetRecord().getRecord().getHeader();
    verifyIdentifiers(Collections.singletonList(recordHeader), Collections.singletonList("00000000-0000-4a89-a2f9-78ce3145e4fc"));
    assertThat(oaiPmhResponseWithExistingIdentifier.getGetRecord(), is(notNullValue()));
    assertThat(oaiPmhResponseWithExistingIdentifier.getErrors(), is(empty()));
  }

  @ParameterizedTest
  @EnumSource(MetadataPrefix.class)
  void getOaiGetRecordVerbWithNonExistingIdentifier(MetadataPrefix metadataPrefix) {
    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.NON_EXISTING_IDENTIFIER;
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, GET_RECORD.value())
      .param(IDENTIFIER_PARAM, identifier)
      .param(METADATA_PREFIX_PARAM, metadataPrefix.getName());

    OAIPMH oaipmh = verifyResponseWithErrors(request, GET_RECORD, 404, 1);
    assertThat(oaipmh.getGetRecord(), is(nullValue()));
    assertThat(oaipmh.getErrors().get(0).getCode(), equalTo(ID_DOES_NOT_EXIST));

  }

  @Test
  void getOaiMetadataFormats(VertxTestContext testContext) {
    getLogger().info("=== Test Metadata Formats without identifier ===");
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_METADATA_FORMATS.value());

    OAIPMH oaiPmhResponseWithoutIdentifier = verify200WithXml(request, LIST_METADATA_FORMATS);

    assertThat(oaiPmhResponseWithoutIdentifier.getListMetadataFormats(), is(notNullValue()));
    assertThat(oaiPmhResponseWithoutIdentifier.getErrors(), is(empty()));

    testContext.completeNow();
  }

  @Test
  void getOaiMetadataFormatsWithExistingIdentifier(VertxTestContext testContext) {
    getLogger().info("=== Test Metadata Formats with existing identifier ===");

    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.EXISTING_IDENTIFIER;
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_METADATA_FORMATS.value())
      .param(IDENTIFIER_PARAM, identifier);

    OAIPMH oaiPmhResponseWithExistingIdentifier = verify200WithXml(request, LIST_METADATA_FORMATS);

    assertThat(oaiPmhResponseWithExistingIdentifier.getListMetadataFormats(), is(notNullValue()));
    assertThat(oaiPmhResponseWithExistingIdentifier.getErrors(), is(empty()));

    testContext.completeNow();
  }

  @Test
  void getOaiMetadataFormatsWithNonExistingIdentifier(VertxTestContext testContext) {
    getLogger().info("=== Test Metadata Formats with non-existing identifier ===");

    // Check that error message is returned
    String identifier = IDENTIFIER_PREFIX + OkapiMockServer.NON_EXISTING_IDENTIFIER;
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_METADATA_FORMATS.value())
      .param(IDENTIFIER_PARAM, identifier);

    OAIPMH oaipmh = verifyResponseWithErrors(request, LIST_METADATA_FORMATS, 404, 1);

    assertThat(oaipmh.getListMetadataFormats(), is(nullValue()));
    assertThat(oaipmh.getErrors().get(0).getCode(), equalTo(ID_DOES_NOT_EXIST));

    testContext.completeNow();
  }

  @Test
  void getOaiMetadataFormatsWithErrorFromStorage(VertxTestContext testContext) {
    getLogger().info("=== Test Metadata Formats with expected error from storage service ===");
    // Check that error message is returned
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_METADATA_FORMATS.value())
      .param(IDENTIFIER_PARAM, IDENTIFIER_PREFIX + OkapiMockServer.ERROR_IDENTIFIER);

    verify500WithErrorMessage(request);

    testContext.completeNow();
  }

  @Test
  void getOaiMetadataFormatsWithInvalidIdentifier(VertxTestContext testContext) {
    getLogger().info("=== Test Metadata Formats with invalid identifier format ===");

    // Check that error message is returned
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_METADATA_FORMATS.value())
      .param(IDENTIFIER_PARAM, OkapiMockServer.INVALID_IDENTIFIER);

    OAIPMH oaipmh = verifyResponseWithErrors(request, LIST_METADATA_FORMATS, 400, 1);

    assertThat(oaipmh.getListMetadataFormats(), is(nullValue()));
    assertThat(oaipmh.getErrors().get(0).getCode(), equalTo(BAD_ARGUMENT));

    testContext.completeNow();
  }

  @Test
  void testSuccessfulGetOaiSets(VertxTestContext testContext) {
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_SETS.value());

    OAIPMH oaipmhFromString = verify200WithXml(request, LIST_SETS);

    assertThat(oaipmhFromString.getListSets(), is(notNullValue()));
    assertThat(oaipmhFromString.getListSets().getSets(), hasSize(equalTo(1)));
    assertThat(oaipmhFromString.getListSets().getSets().get(0).getSetSpec(), equalTo("all"));
    assertThat(oaipmhFromString.getListSets().getSets().get(0).getSetName(), equalTo("All records"));

    testContext.completeNow();
  }

  @Test
  void testGetOaiSetsWithResumptionToken(VertxTestContext testContext) {
    String resumptionToken = "abc";
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, LIST_SETS.value())
      .param(RESUMPTION_TOKEN_PARAM, resumptionToken);

    OAIPMH oai = verifyResponseWithErrors(request, LIST_SETS, 400, 1);

    assertThat(oai.getErrors().get(0).getCode(), is(equalTo(BAD_RESUMPTION_TOKEN)));
    assertThat(oai.getRequest().getResumptionToken(), is(equalTo(resumptionToken)));

    testContext.completeNow();
  }

  @Test
  void getOaiRepositoryInfoSuccess(VertxTestContext testContext) {
    RequestSpecification request = createBaseRequest(RECORDS_PATH)
      .with()
      .param(VERB_PARAM, IDENTIFY.value());

    OAIPMH oaipmhFromString = verify200WithXml(request, IDENTIFY);

    verifyRepositoryInfoResponse(oaipmhFromString);

    testContext.completeNow();
  }

  @ParameterizedTest
  @ValueSource(strings = { REPOSITORY_ADMIN_EMAILS, REPOSITORY_NAME })
  void getOaiRepositoryInfoMissingRequiredConfigs(String propKey) {
    String prop = System.clearProperty(propKey);
    RequestSpecification request = createBaseRequest(RECORDS_PATH, tenantWithotConfigsHeader)
      .with()
      .param(VERB_PARAM, IDENTIFY.value());

    try {
      verify500WithErrorMessage(request);
    } finally {
      System.setProperty(propKey, prop);
    }
  }

  private RequestSpecification createBaseRequest(String basePath) {
    return createBaseRequest(basePath, tenantHeader);
  }

  private RequestSpecification createBaseRequest(String basePath, Header tenant) {
    return RestAssured
      .given()
        .header(okapiUrlHeader)
        .header(tokenHeader)
        .header(tenant)
        .basePath(basePath)
        .contentType(XML_TYPE);
  }

  private void verifyBaseResponse(OAIPMH oaipmhFromString, VerbType verb) {
    assertThat(oaipmhFromString, is(notNullValue()));
    assertThat(oaipmhFromString.getResponseDate(), is(notNullValue()));
    assertThat(oaipmhFromString.getResponseDate().isBefore(Instant.now()), is(true));
    assertThat(oaipmhFromString.getRequest(), is(notNullValue()));
    assertThat(oaipmhFromString.getRequest().getValue(), is(notNullValue()));
    assertThat(oaipmhFromString.getRequest().getVerb(), equalTo(verb));
  }

  private OAIPMH verify200WithXml(RequestSpecification request, VerbType verb) {
    String response = verifyWithCodeWithXml(request, 200);

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmh = ResponseConverter.getInstance().stringToOaiPmh(response);

    verifyBaseResponse(oaipmh, verb);

    return oaipmh;
  }

  private String verifyWithCodeWithXml(RequestSpecification request, int code) {
    ValidatableResponse response = request
      .when()
        .get()
      .then()
        .statusCode(code)
        .contentType(XML_TYPE);

    return response
      .extract()
        .body()
          .asString();
  }

  private void verify500WithErrorMessage(RequestSpecification request) {
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
  }

  private void verifyRepositoryInfoResponse(OAIPMH oaipmhFromString) {
    assertThat(oaipmhFromString.getIdentify(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getBaseURL(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getAdminEmails(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getAdminEmails(), hasSize(equalTo(2)));
    assertThat(oaipmhFromString.getIdentify().getEarliestDatestamp(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getGranularity(), is(equalTo(GranularityType.YYYY_MM_DD_THH_MM_SS_Z)));
    assertThat(oaipmhFromString.getIdentify().getProtocolVersion(), is(equalTo(Constants.REPOSITORY_PROTOCOL_VERSION_2_0)));
    assertThat(oaipmhFromString.getIdentify().getRepositoryName(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getCompressions(), is(notNullValue()));
    assertThat(oaipmhFromString.getIdentify().getCompressions(), containsInAnyOrder(GZIP, DEFLATE));
    assertThat(oaipmhFromString.getIdentify().getDescriptions(), hasSize(equalTo(1)));
    assertThat(oaipmhFromString.getIdentify().getDescriptions().get(0).getAny(), instanceOf(OaiIdentifier.class));
  }

  private OAIPMH verifyResponseWithErrors(RequestSpecification request, VerbType verb, int statusCode, int errorsCount) {
    String response = verifyWithCodeWithXml(request, statusCode);

    // Unmarshal string to OAIPMH and verify required data presents
    OAIPMH oaipmhFromString = ResponseConverter.getInstance().stringToOaiPmh(response);

    verifyBaseResponse(oaipmhFromString, verb);
    assertThat(oaipmhFromString.getErrors(), is(notNullValue()));
    assertThat(oaipmhFromString.getErrors(), hasSize(errorsCount));

    return oaipmhFromString;
  }

  private void verifyRecord(RecordType record, MetadataPrefix metadataPrefix) {
    assertThat(record.getMetadata(), is(notNullValue()));
    if (metadataPrefix == MetadataPrefix.MARC21XML) {
      assertThat(record.getMetadata().getAny(), is(instanceOf(gov.loc.marc21.slim.RecordType.class)));
    } else if (metadataPrefix == MetadataPrefix.DC) {
      assertThat(record.getMetadata().getAny(), is(instanceOf(Dc.class)));
    }
    verifyHeader(record.getHeader());
  }

  private void verifyHeader(HeaderType header) {
    assertThat(header.getIdentifier(), containsString(IDENTIFIER_PREFIX));
    assertThat(header.getSetSpecs(), hasSize(1));
    assertThat(header.getDatestamp(), is(notNullValue()));
  }

  private void verifyListResponse(OAIPMH oaipmh, VerbType verb, int recordsCount) {
    assertThat(oaipmh.getErrors(), is(empty()));
    if (verb == LIST_IDENTIFIERS) {
      assertThat(oaipmh.getListIdentifiers(), is(notNullValue()));
      assertThat(oaipmh.getListIdentifiers().getHeaders(), hasSize(recordsCount));
      oaipmh.getListIdentifiers().getHeaders().forEach(this::verifyHeader);
      if(recordsCount==10){
        List<HeaderType> headers = oaipmh.getListIdentifiers().getHeaders();
        verifyIdentifiers(headers, getExpectedIdentifiers());
      }
    } else if (verb == LIST_RECORDS) {
      assertThat(oaipmh.getListRecords(), is(notNullValue()));
      assertThat(oaipmh.getListRecords().getRecords(), hasSize(recordsCount));
      MetadataPrefix metadataPrefix = MetadataPrefix.fromName(oaipmh.getRequest().getMetadataPrefix());
      oaipmh.getListRecords().getRecords().forEach(record -> verifyRecord(record, metadataPrefix));
      if(recordsCount==10){
        List<HeaderType> headers = oaipmh.getListRecords().getRecords().stream()
          .map(RecordType::getHeader)
          .collect(Collectors.toList());
        verifyIdentifiers(headers, getExpectedIdentifiers());
      }
    } else {
      fail("Can't verify specified verb: " + verb);
    }
  }

  private void verifySuppressDiscoveryFieldHasCorrectValue(OAIPMH oaipmh, VerbType verbType, MetadataPrefix metadataPrefix) {
    List<RecordType> records = getListRecords(oaipmh, verbType);
    if(Objects.isNull(records)) {
      fail("Can't verify specified verb: " + verbType);
    }
    records.stream()
      .filter(recordType -> recordType.getHeader().getIdentifier().contains(TEST_INSTANCE_ID))
      .findFirst()
      .ifPresent(
        recordType -> {
          if(metadataPrefix.equals(MetadataPrefix.MARC21XML)) {
            verifyForMarcRecord(recordType);
          } else {
            verifyForDcRecord(recordType);
          }
        }
      );
  }

  private List<RecordType> getListRecords(OAIPMH oaipmh, VerbType verbType) {
    List<RecordType> records;
    if (verbType == LIST_RECORDS) {
      records = oaipmh.getListRecords().getRecords();
    }
    else if (verbType == GET_RECORD){
      records = Collections.singletonList(oaipmh.getGetRecord().getRecord());
    } else {
      return null;
    }
    return records;
  }

  private void verifyForMarcRecord(RecordType record) {
    gov.loc.marc21.slim.RecordType recordType = (gov.loc.marc21.slim.RecordType) record.getMetadata().getAny();
    List<DataFieldType> datafields = recordType.getDatafields();
    datafields.stream()
      .filter(suppressedDiscoveryMarcFieldPredicate)
      .findFirst()
      .ifPresent(dataField -> {
        Optional<SubfieldatafieldType> subfieldOptional = dataField.getSubfields().stream()
          .filter(subfieldatafieldType -> subfieldatafieldType.getCode().equals(SUPPRESS_FROM_DISCOVERY_SUBFIELD_CODE))
          .findFirst();
          if(subfieldOptional.isPresent()){
            assertEquals(TEST_INSTANCE_EXPECTED_VALUE_FOR_MARC21, subfieldOptional.get().getValue());
          } else {
            fail("Record has incorrect structure: datafield 999 is absence");
          }
      });
  }

  private void verifyForDcRecord(RecordType record) {
    Dc dc = (Dc) record.getMetadata().getAny();
    dc.getTitlesAndCreatorsAndSubjects().stream()
      .filter(suppressedDiscoveryDcFieldPredicate)
      .findFirst()
      .ifPresent(jaxbElement -> {
        String value = jaxbElement.getValue().getValue();
        assertEquals(TEST_INSTANCE_EXPECTED_VALUE_FOR_DC, value);
      });
  }

  private void verifySuppressedDiscoveryFieldPresence(OAIPMH oaipmh, VerbType verbType, MetadataPrefix metadataPrefix, boolean shouldContainField) {
    List<RecordType> records = getListRecords(oaipmh, verbType);
    if(Objects.isNull(records)) {
      fail("Can't verify specified verb: " + verbType);
    }
    if (metadataPrefix.equals(MetadataPrefix.MARC21XML)) {
      verifySuppressedDiscoveryDataFieldForMarcRecords(records, shouldContainField);
    } else {
      verifySuppressedDiscoveryDataFieldForDcRecords(records, shouldContainField);
    }
  }

  private void verifySuppressedDiscoveryDataFieldForMarcRecords(List<RecordType> records, boolean shouldContainField){
    records.forEach(record -> {
      gov.loc.marc21.slim.RecordType recordType = (gov.loc.marc21.slim.RecordType) record.getMetadata().getAny();
      List<DataFieldType> datafields = recordType.getDatafields();
      boolean isRecordCorrect;
      Stream<DataFieldType> stream = datafields.stream();
      if (shouldContainField) {
        isRecordCorrect = stream
          .anyMatch(suppressedDiscoveryMarcFieldPredicate);
      } else {
        isRecordCorrect = stream
          .noneMatch(suppressedDiscoveryMarcFieldPredicate);
      }
      assertTrue(isRecordCorrect);
    });
  }

  private void verifySuppressedDiscoveryDataFieldForDcRecords(List<RecordType> records, boolean shouldContainField){
    records.forEach(record -> {
      Dc dc = (Dc) record.getMetadata().getAny();
      boolean isRecordCorrect;
      Stream<JAXBElement<ElementType>> stream = dc.getTitlesAndCreatorsAndSubjects().stream();
      if (shouldContainField) {
        isRecordCorrect = stream
          .anyMatch(suppressedDiscoveryDcFieldPredicate);
      } else {
        isRecordCorrect = stream
          .noneMatch(suppressedDiscoveryDcFieldPredicate);
      }
      assertTrue(isRecordCorrect);
    });
  }

  private ResumptionTokenType getResumptionToken(OAIPMH oaipmh, VerbType verb) {
    if (verb == LIST_IDENTIFIERS) {
      return oaipmh.getListIdentifiers().getResumptionToken();
    } else if (verb == LIST_RECORDS) {
      return oaipmh.getListRecords().getResumptionToken();
    } else {
      return null;
    }
  }

  private String getParamValue(List<NameValuePair> params, String name) {
    return params.stream()
      .filter(p -> p.getName().equals(name))
      .map(NameValuePair::getValue)
      .findFirst()
      .get();
  }

  private RequestSpecification addAcceptEncodingHeader(String encoding) {
    return addAcceptEncodingHeader(given(), encoding);
  }

  private RequestSpecification addAcceptEncodingHeader(RequestSpecification requestSpecification, String encoding) {
    RestAssuredConfig config = RestAssuredConfig.newConfig();
    DecoderConfig decoderConfig = DecoderConfig.decoderConfig();
    if ("IDENTITY".equals(encoding)) {
      config = config.decoderConfig(decoderConfig.noContentDecoders());
      requestSpecification
        .with()
        .header(new Header(String.valueOf(HttpHeaders.ACCEPT_ENCODING), encoding));
    } else {
      config = config.decoderConfig(decoderConfig.contentDecoders(ContentDecoder.valueOf(encoding)));
    }

    return requestSpecification.config(config);
  }

  private void verifyContentEncodingHeader(ValidatableResponse response) {
    String acceptEncoding = response.extract()
                            .header(String.valueOf(HttpHeaders.ACCEPT_ENCODING));
    if (acceptEncoding == null) {
      response.header(String.valueOf(HttpHeaders.CONTENT_ENCODING), nullValue());
    } else {
      List<String> values = Arrays.stream(acceptEncoding.split(","))
                                  .map(String::toLowerCase)
                                  .collect(Collectors.toList());
      if (values.contains("identity")) {
        response.header(String.valueOf(HttpHeaders.CONTENT_ENCODING), nullValue());
      } else {
        response.header(String.valueOf(HttpHeaders.CONTENT_ENCODING).toLowerCase(), isIn(values));
      }
    }
  }

  private static Stream<Arguments> metadataPrefixAndEncodingProvider() {
    Stream.Builder<Arguments> builder = Stream.builder();
    for (MetadataPrefix prefix : MetadataPrefix.values()) {
      for (String encoding : ENCODINGS) {
        builder.add(Arguments.arguments(prefix, encoding));
      }
    }
    return builder.build();
  }

  private static Stream<Arguments> metadataPrefixAndVerbProvider() {
    Stream.Builder<Arguments> builder = Stream.builder();
    for (MetadataPrefix prefix : MetadataPrefix.values()) {
      for (VerbType verb : LIST_VERBS) {
        builder.add(Arguments.arguments(prefix, verb));
      }
    }
    return builder.build();
  }

  private void verifyIdentifiers(List<HeaderType> headers, List<String> expectedIdentifiers) {
    List<String> headerIdentifiers = headers.stream()
      .map(this::getUUIDofHeaderIdentifier)
      .collect(Collectors.toList());
    assertTrue(headerIdentifiers.containsAll(expectedIdentifiers));
  }

  private String getUUIDofHeaderIdentifier(HeaderType header) {
    String identifierWithPrefix = header.getIdentifier();
    return identifierWithPrefix.substring(IDENTIFIER_PREFIX.length());
  }

  private List<String> getExpectedIdentifiers() {
    //@formatter:of
    return Arrays.asList(
      "00000000-0000-4000-a000-000000000000",
      "10000000-0000-4000-a000-000000000000",
      "20000000-0000-4000-a000-000000000000",
      "30000000-0000-4000-a000-000000000000",
      "40000000-0000-4000-a000-000000000000",
      "50000000-0000-4000-a000-000000000000",
      "60000000-0000-4000-a000-000000000000",
      "70000000-0000-4000-a000-000000000000",
      "80000000-0000-4000-a000-000000000000",
      "90000000-0000-4000-a000-000000000000");
    //@formatter:on
  }
}
