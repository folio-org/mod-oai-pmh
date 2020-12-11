package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.openarchives.oai._2.ListIdentifiersType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.ResumptionTokenType;

import javax.ws.rs.core.Response;
import java.util.List;

import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FLOW_ERROR;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

public class GetOaiIdentifiersHelper extends AbstractGetRecordsHelper {

  private static final Logger logger = LoggerFactory.getLogger(GetOaiIdentifiersHelper.class);

  @Override
  public Future<javax.ws.rs.core.Response> handle(Request request, Context ctx) {
    Promise<javax.ws.rs.core.Response> promise = Promise.promise();
    try {
      ResponseHelper responseHelper = getResponseHelper();
      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        OAIPMH oai;
        if (request.isRestored()) {
          oai = responseHelper.buildOaipmhResponseWithErrors(request, BAD_RESUMPTION_TOKEN, RESUMPTION_TOKEN_FORMAT_ERROR);
        } else {
          oai = responseHelper.buildOaipmhResponseWithErrors(request, errors);
        }
        promise.complete(getResponseHelper().buildFailureResponse(oai, request));
        return promise.future();
      }
      requestAndProcessSrsRecords(request, ctx, promise);
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  @Override
  protected List<OAIPMHerrorType> validateRequest(Request request) {
    return validateListRequest(request);
  }

  @Override
  protected Response processRecords(Context ctx, Request request, JsonObject srsRecords) {
    OAIPMH oaipmh = buildListIdentifiers(request, srsRecords);
    return buildResponse(oaipmh, request);
  }

  /**
   * Check if there are identifiers built and construct success response, otherwise return response with error(s)
   */
  @Override
  protected javax.ws.rs.core.Response buildResponse(OAIPMH oai, Request request) {
    if (oai.getListIdentifiers() == null) {
      return getResponseHelper().buildFailureResponse(oai, request);
    } else {
      return getResponseHelper().buildSuccessResponse(oai);
    }
  }

  /**
   * Builds {@link ListIdentifiersType} with headers if there is any item or {@code null}
   *
   * @param request           request
   * @param srsRecords the response from the storage which contains items
   * @return {@link ListIdentifiersType} with headers if there is any or {@code null}
   */
  private OAIPMH buildListIdentifiers(Request request, JsonObject srsRecords) {

    ResponseHelper responseHelper = getResponseHelper();
    JsonArray instances = storageHelper.getItems(srsRecords);
    Integer totalRecords = storageHelper.getTotalRecords(srsRecords);
    if (request.isRestored() && !canResumeRequestSequence(request, totalRecords, instances)) {
      return responseHelper.buildOaipmhResponseWithErrors(request, BAD_RESUMPTION_TOKEN, RESUMPTION_TOKEN_FLOW_ERROR);
    }
    if (instances != null && !instances.isEmpty()) {
      logger.debug("{} entries retrieved out of {}", instances.size(), totalRecords);

      ListIdentifiersType identifiers = new ListIdentifiersType()
        .withResumptionToken(buildResumptionToken(request, instances, totalRecords));

      String identifierPrefix = request.getIdentifierPrefix();
      instances.stream()
        .map(object -> (JsonObject) object)
        .filter(instance -> StringUtils.isNotEmpty(storageHelper.getIdentifierId(instance)))
        .filter(instance -> filterInstance(request, instance))
        .map(instance -> addHeader(identifierPrefix, request, instance))
        .forEach(identifiers::withHeaders);

      if (identifiers.getHeaders().isEmpty()) {
        OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request);
        return oaipmh.withErrors(createNoRecordsFoundError());
      }
      ResumptionTokenType resumptionToken = buildResumptionToken(request, instances, totalRecords);
      OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request).withListIdentifiers(identifiers);
      addResumptionTokenToOaiResponse(oaipmh, resumptionToken);
      return oaipmh;
    }
    return responseHelper.buildOaipmhResponseWithErrors(request, createNoRecordsFoundError());
  }

  @Override
  protected void addResumptionTokenToOaiResponse(OAIPMH oaipmh, ResumptionTokenType resumptionToken) {
    if (oaipmh.getListRecords() != null) {
      oaipmh.getListIdentifiers()
        .withResumptionToken(resumptionToken);
    }
  }

}
