package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.openarchives.oai._2.ListSetsType;
import org.openarchives.oai._2.OAIPMH;

import javax.ws.rs.core.Response;

import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

/**
 * Helper class that contains business logic for retrieving OAI-PMH set structure of a repository.
 */
public class GetOaiSetsHelper extends AbstractHelper {

  private static final Logger logger = LogManager.getLogger(GetOaiSetsHelper.class);

  @Override
  public Future<Response> handle(Request request, Context ctx) {
    Promise<Response> promise = Promise.promise();
    ResponseHelper responseHelper = getResponseHelper();
    try {
      OAIPMH oai;
      if (request.getResumptionToken() != null) {
        oai = responseHelper.buildOaipmhResponseWithErrors(request, BAD_RESUMPTION_TOKEN, RESUMPTION_TOKEN_FORMAT_ERROR);
        promise.complete(responseHelper.buildFailureResponse(oai, request));
        return promise.future();
      }
      oai = responseHelper.buildBaseOaipmhResponse(request).withListSets(new ListSetsType()
        .withSets(getSupportedSetTypes()));
      promise.complete(responseHelper.buildSuccessResponse(oai));
    } catch (Exception e) {
      logger.warn("handle:: For requestId {} error happened while processing ListSets verb request {}", request.getRequestId(), e.getMessage());
      promise.fail(e);
    }
    return promise.future();
  }
}
