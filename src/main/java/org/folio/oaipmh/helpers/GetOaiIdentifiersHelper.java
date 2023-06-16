package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.ResumptionTokenType;

import javax.ws.rs.core.Response;
import java.util.List;

import static org.folio.oaipmh.Constants.REPOSITORY_RECORDS_SOURCE;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.INVENTORY;
import static org.folio.oaipmh.Constants.SRS_AND_INVENTORY;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getProperty;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

public class GetOaiIdentifiersHelper extends AbstractGetRecordsHelper {

  @Override
  public Future<javax.ws.rs.core.Response> handle(Request request, Context ctx) {
    Promise<Response> promise = Promise.promise();
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
      var recordsSource = getProperty(request.getRequestId(), REPOSITORY_RECORDS_SOURCE);
      if (recordsSource.equals(INVENTORY)) {
        requestAndProcessInventoryRecords(request, ctx, promise);
      } else {
        requestAndProcessSrsRecords(request, ctx, promise, recordsSource.equals(SRS_AND_INVENTORY));
      }
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
  protected Future<Response> processRecords(Context ctx, Request request, JsonObject srsRecords, JsonObject inventoryRecords) {
    OAIPMH oaipmhResult = buildListIdentifiers(request, srsRecords, inventoryRecords);
    return Future.succeededFuture(buildResponse(oaipmhResult, request));
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

  @Override
  protected void addResumptionTokenToOaiResponse(OAIPMH oaipmh, ResumptionTokenType resumptionToken) {
    if (oaipmh.getListRecords() != null) {
      oaipmh.getListIdentifiers()
        .withResumptionToken(resumptionToken);
    }
  }

}
