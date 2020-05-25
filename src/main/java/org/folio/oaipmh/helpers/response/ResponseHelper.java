package org.folio.oaipmh.helpers.response;

import static org.folio.oaipmh.Constants.REPOSITORY_ERRORS_PROCESSING;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_VERB;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.ID_DOES_NOT_EXIST;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.NO_RECORDS_MATCH;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.SERVICE_UNAVAILABLE;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import org.apache.http.HttpStatus;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseConverter;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;

import com.google.common.collect.ImmutableSet;

/**
 * Used for building base {@link OAIPMH} response or with particular {@link OAIPMHerrorType} error(s).
 * As well build success and failure {@link Response}.
 */
public class ResponseHelper {
  /**
   * Contains errors that should be responded with 400 http status code
   */
  private final Set<OAIPMHerrorcodeType> badRequestStatusErrors = ImmutableSet.of(BAD_ARGUMENT, BAD_RESUMPTION_TOKEN, BAD_VERB);
  /**
   * Contains errors that should be responded with 404 http status code
   */
  private final Set<OAIPMHerrorcodeType> notFoundStatusErrors = ImmutableSet.of(ID_DOES_NOT_EXIST, NO_RECORDS_MATCH);

  private static ResponseHelper instance;

  public static ResponseHelper getInstance() {
    if(Objects.nonNull(instance)){
      return instance;
    }
    instance = new ResponseHelper();
    return instance;
  }

  /**
   * Creates basic {@link OAIPMH} with ResponseDate and Request details
   *
   * @param request {@link Request}
   * @return basic {@link OAIPMH}
   */
  public OAIPMH buildBaseOaipmhResponse(Request request) {
    return new OAIPMH().withResponseDate(Instant.now()
      .truncatedTo(ChronoUnit.SECONDS))
      .withRequest(request.getOaiRequest());
  }

  /**
   * Builds base {@link OAIPMH} response with specified {@link OAIPMHerrorType} error.
   *
   * @param request - OAIPMH request
   * @param oaipmhErrorType - OAIPMH error
   * @return OAIPMH response with error
   */
  public OAIPMH buildOaipmhResponseWithErrors(Request request, OAIPMHerrorType oaipmhErrorType) {
    return buildBaseOaipmhResponse(request).withErrors(oaipmhErrorType);
  }

  /**
   * Builds base {@link OAIPMH} response with error which has specified code and value(message).
   *
   * @param request - OAIPMH request
   * @param errorCode - OAIPMH errorCode
   * @param message - error message
   * @return OAIPMH response with error
   */
  public OAIPMH buildOaipmhResponseWithErrors(Request request, OAIPMHerrorcodeType errorCode, String message) {
    return buildBaseOaipmhResponse(request).withErrors(new OAIPMHerrorType().withCode(errorCode)
      .withValue(message));
  }

  /**
   * Builds base {@link OAIPMH} response with specified errors list.
   *
   * @param request - OAIPMH request
   * @param errors - list of errors
   * @return OAIPMH response with error
   */
  public OAIPMH buildOaipmhResponseWithErrors(Request request, List<OAIPMHerrorType> errors) {
    return buildBaseOaipmhResponse(request).withErrors(errors);
  }

  /**
   * Builds response with 200 status code and with body of {@link OAIPMH} object.
   *
   * @param oaipmh - OAIPMH response object
   * @return built response
   */
  public Response buildSuccessResponse(OAIPMH oaipmh) {
    return respondWithTextXml(HttpStatus.SC_OK, ResponseConverter.getInstance()
      .convertToString(oaipmh));
  }

  /**
   * Builds 'failure' {@link Response} with OAIPMH as a body and with status code depending on repository.errorsProcessing setting.
   * If setting has true value then errors that refers to OAIPMH level errors will be responded with 200 status code and with their
   * particular 4** status code in opposite case. In all other cases the response with 500 status code is built as default one.
   *
   * @param oaipmh - OAIPMH response
   * @param request - OAIPMH request
   * @return built response
   */
  public Response buildFailureResponse(OAIPMH oaipmh, Request request) {
    String responseBody = ResponseConverter.getInstance()
      .convertToString(oaipmh);
    boolean shouldRespondWithStatusOk = shouldRespondWithStatusOk(request);
    Set<OAIPMHerrorcodeType> errorCodes = getErrorCodes(oaipmh);
      // 400
    if (!Collections.disjoint(errorCodes, badRequestStatusErrors)) {
      int statusCode = shouldRespondWithStatusOk ? HttpStatus.SC_OK : HttpStatus.SC_BAD_REQUEST;
      return respondWithTextXml(statusCode, responseBody);
      // 404
    } else if (!Collections.disjoint(errorCodes, notFoundStatusErrors)) {
      int statusCode = shouldRespondWithStatusOk ? HttpStatus.SC_OK : HttpStatus.SC_NOT_FOUND;
      return respondWithTextXml(statusCode, responseBody);
      // 422
    } else if (errorCodes.contains(CANNOT_DISSEMINATE_FORMAT)) {
      int statusCode = shouldRespondWithStatusOk ? HttpStatus.SC_OK : HttpStatus.SC_UNPROCESSABLE_ENTITY;
      return respondWithTextXml(statusCode, responseBody);
      // 503
    } else if (getErrorCodes(oaipmh).contains(SERVICE_UNAVAILABLE)) {
      return respondWithTextXml(HttpStatus.SC_SERVICE_UNAVAILABLE, responseBody);
    }
    return respondWithTextXml(HttpStatus.SC_NOT_FOUND, responseBody);
  }

  /**
   * The error codes required to define the http code to be returned in the http response
   *
   * @param oai OAIPMH response with errors
   * @return set of error codes
   */
  private Set<OAIPMHerrorcodeType> getErrorCodes(OAIPMH oai) {
    // According to oai-pmh.raml the service will return different http codes depending on the error
    return oai.getErrors()
      .stream()
      .map(OAIPMHerrorType::getCode)
      .collect(Collectors.toSet());
  }

  /**
   * Returns value of repository.errorsProcessing setting
   *
   * @param request - OAIPMH request
   * @return boolean
   */
  private boolean shouldRespondWithStatusOk(Request request) {
    String tenant = TenantTool.tenantId(request.getOkapiHeaders());
    String config = RepositoryConfigurationUtil.getProperty(tenant, REPOSITORY_ERRORS_PROCESSING);
    return config.equals("200");
  }

  /**
   * Builds {@link Response} object with specified status code and response body.
   * Response has header with 'Content-Type' = 'text/xml'.
   *
   * @param statusCode - status code
   * @param responseBody - converted to string OAIPMH response
   * @return built response
   */
  private Response respondWithTextXml(int statusCode, String responseBody) {
    Response.ResponseBuilder responseBuilder = Response.status(statusCode)
      .header("Content-Type", "text/xml").entity(responseBody);
    return responseBuilder.build();
  }

}
