package org.folio.oaipmh.helpers.response;

import static org.folio.oaipmh.Constants.REPOSITORY_ERRORS_PROCESSING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_VERB;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.ID_DOES_NOT_EXIST;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.NO_SET_HIERARCHY;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.SERVICE_UNAVAILABLE;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.http.HttpStatus;
import org.folio.oaipmh.Request;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openarchives.oai._2.ListSetsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.SetType;

import com.google.common.collect.ImmutableMap;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
class ResponseHelperTest {

  private static final String RESPOND_WITH_OK = "200";
  private static final String RESPOND_WITH_ERROR = "500";
  private static final String TEST_ERROR_MESSAGE = "test error message";
  private static final String TEXT_XML = "text/xml";

  private static final OAIPMHerrorType TEST_ERROR = new OAIPMHerrorType().withCode(BAD_ARGUMENT)
    .withValue(TEST_ERROR_MESSAGE);
  private static final List<OAIPMHerrorType> TEST_ERROR_LIST = Collections.singletonList(TEST_ERROR);
  private static final String TEST_VALUE = "Test";

  private ResponseHelper responseHelper = new ResponseHelper();
  private static Map<OAIPMHerrorcodeType, Integer> errorTypeToStatusCodeMap = new HashMap<>();
  private static Request request;

  @BeforeAll
  static void setUpOnce() {
    Map<String, String> okapiHeaders = ImmutableMap.of("tenant", "diku");
    request = Request.builder()
      .okapiHeaders(okapiHeaders)
      .build();
    initErrorCodeToStatusCodeMap();
  }

  private static void initErrorCodeToStatusCodeMap() {
    errorTypeToStatusCodeMap.put(BAD_ARGUMENT, HttpStatus.SC_BAD_REQUEST);
    errorTypeToStatusCodeMap.put(ID_DOES_NOT_EXIST, HttpStatus.SC_NOT_FOUND);
    errorTypeToStatusCodeMap.put(CANNOT_DISSEMINATE_FORMAT, HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  void shouldBuildBaseOaipmhResponse() {
    OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request);

    assertNotNull(oaipmh.getResponseDate());
    assertEquals(oaipmh.getRequest(), request.getOaiRequest());
  }

  @Test
  void shouldBuildOaipmhResponseWithError() {
    OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, TEST_ERROR);

    assertNotNull(oaipmh.getResponseDate());
    assertEquals(oaipmh.getRequest(), request.getOaiRequest());
    assertThat(oaipmh.getErrors(), contains(TEST_ERROR));
  }

  @Test
  void shouldBuildOaipmhResponseWithErrorCodeAndValue() {
    OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, BAD_VERB, TEST_ERROR_MESSAGE);

    boolean isErrorPresented = oaipmh.getErrors()
      .stream()
      .anyMatch(error -> error.getCode().equals(BAD_VERB) && error.getValue().equals(TEST_ERROR_MESSAGE));
    assertTrue(isErrorPresented);
    assertNotNull(oaipmh.getResponseDate());
    assertEquals(oaipmh.getRequest(), request.getOaiRequest());
  }

  @Test
  void shouldBuildOaipmhResponseWithErrorsList() {
    OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, TEST_ERROR_LIST);

    assertNotNull(oaipmh.getResponseDate());
    assertEquals(oaipmh.getRequest(), request.getOaiRequest());
    assertEquals(TEST_ERROR_LIST, oaipmh.getErrors());
  }

  @Test
  void shouldBuildResponseWithStatusCodeOk(Vertx vertx, VertxTestContext testContext) {
    setupErrorsProcessingSettingWithValue(RESPOND_WITH_OK);
    Map<OAIPMHerrorcodeType, OAIPMH> responsesWithErrors = buildOaipmhResponsesWithError();
    vertx.runOnContext(event -> testContext.verify(() -> {
      responsesWithErrors.forEach((error, oaipmh) -> {
        Response response = responseHelper.buildFailureResponse(oaipmh, request);
        verifyResponse(response, HttpStatus.SC_OK);
      });
      testContext.completeNow();
    }));
  }

  @Test
  void shouldBuildResponseWithErrorStatusCode(Vertx vertx, VertxTestContext testContext) {
    setupErrorsProcessingSettingWithValue(RESPOND_WITH_ERROR);
    Map<OAIPMHerrorcodeType, OAIPMH> responsesWithErrors = buildOaipmhResponsesWithError();
    vertx.runOnContext(event -> testContext.verify(() -> {
      responsesWithErrors.forEach((error, oaipmh) -> {
        Response response = responseHelper.buildFailureResponse(oaipmh, request);
        int expectedStatusCode = errorTypeToStatusCodeMap.get(error);
        verifyResponse(response, expectedStatusCode);
      });
      testContext.completeNow();
    }));
  }

  @Test
  void shouldBuildResponseWithServiceUnavailableStatusCode(Vertx vertx, VertxTestContext testContext) {
    setupErrorsProcessingSettingWithValue(RESPOND_WITH_ERROR);
    vertx.runOnContext(event -> testContext.verify(() -> {
      OAIPMHerrorType serviceUnavailable = new OAIPMHerrorType().withCode(SERVICE_UNAVAILABLE)
        .withValue(TEST_ERROR_MESSAGE);
      OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, serviceUnavailable);
      Response response = responseHelper.buildFailureResponse(oaipmh, request);
      verifyResponse(response, HttpStatus.SC_SERVICE_UNAVAILABLE);
      testContext.completeNow();
    }));
  }

  @Test
  void shouldBuildResponseWithNotFoundStatusCodeAsDefault(Vertx vertx, VertxTestContext testContext) {
    setupErrorsProcessingSettingWithValue(RESPOND_WITH_ERROR);
    vertx.runOnContext(event -> testContext.verify(() -> {
      OAIPMHerrorType errorTypeForDefaultStatusCode = new OAIPMHerrorType().withCode(NO_SET_HIERARCHY)
        .withValue(TEST_ERROR_MESSAGE);
      OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, errorTypeForDefaultStatusCode);
      Response response = responseHelper.buildFailureResponse(oaipmh, request);
      verifyResponse(response, HttpStatus.SC_NOT_FOUND);
      testContext.completeNow();
    }));
  }

  @Test
  void shouldBuildSuccessResponse() {
    OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request);
    oaipmh.withListSets(new ListSetsType().withSets(new SetType().withSetName("all")
      .withSetSpec(TEST_VALUE)));

    Response response = responseHelper.buildSuccessResponse(oaipmh);

    verifyResponse(response, HttpStatus.SC_OK);
  }

  private void verifyResponse(Response response, int expectedStatusCode) {
    String contentType = response.getHeaders()
      .get("Content-Type")
      .iterator()
      .next()
      .toString();

    assertEquals(TEXT_XML, contentType);
    assertEquals(expectedStatusCode, response.getStatus());
    assertTrue(response.hasEntity());
  }

  private void setupErrorsProcessingSettingWithValue(String value) {
    System.setProperty(REPOSITORY_ERRORS_PROCESSING, value);
  }

  private Map<OAIPMHerrorcodeType, OAIPMH> buildOaipmhResponsesWithError() {
    OAIPMHerrorType badArgumentError = new OAIPMHerrorType().withCode(BAD_ARGUMENT)
      .withValue(TEST_ERROR_MESSAGE);
    OAIPMHerrorType notFoundError = new OAIPMHerrorType().withCode(ID_DOES_NOT_EXIST)
      .withValue(TEST_ERROR_MESSAGE);
    OAIPMHerrorType cannotDisseminateFormatError = new OAIPMHerrorType().withCode(CANNOT_DISSEMINATE_FORMAT)
      .withValue(TEST_ERROR_MESSAGE);

    Map<OAIPMHerrorcodeType, OAIPMH> responsesWithErrors = new HashMap<>();
    responsesWithErrors.put(BAD_ARGUMENT, responseHelper.buildOaipmhResponseWithErrors(request, badArgumentError));
    responsesWithErrors.put(ID_DOES_NOT_EXIST, responseHelper.buildOaipmhResponseWithErrors(request, notFoundError));
    responsesWithErrors.put(CANNOT_DISSEMINATE_FORMAT,
        responseHelper.buildOaipmhResponseWithErrors(request, cannotDisseminateFormatError));

    return responsesWithErrors;
  }

}
