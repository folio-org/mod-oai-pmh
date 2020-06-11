package org.folio.oaipmh.helpers;

import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

import javax.ws.rs.core.Response;

import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.openarchives.oai._2.ListSetsType;
import org.openarchives.oai._2.OAIPMH;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Helper class that contains business logic for retrieving OAI-PMH set structure of a repository.
 */
public class GetOaiSetsHelper extends AbstractHelper {
  private static final Logger logger = LoggerFactory.getLogger(GetOaiSetsHelper.class);

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
      logger.error("Error happened while processing ListSets verb request", e);
      promise.fail(e);
    }
    return promise.future();
  }
}
