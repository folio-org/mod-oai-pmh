package org.folio.oaipmh.helpers;

import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

import java.util.concurrent.CompletableFuture;

import javax.ws.rs.core.Response;

import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.openarchives.oai._2.ListSetsType;
import org.openarchives.oai._2.OAIPMH;

import io.vertx.core.Context;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;

/**
 * Helper class that contains business logic for retrieving OAI-PMH set structure of a repository.
 */
public class GetOaiSetsHelper extends AbstractHelper {
  private static final Logger logger = LoggerFactory.getLogger(GetOaiSetsHelper.class);

  @Override
  public CompletableFuture<Response> handle(Request request, Context ctx) {
    CompletableFuture<Response> future = new VertxCompletableFuture<>(ctx);
    ResponseHelper responseHelper = getResponseHelper();
    try {
      OAIPMH oai;
      if (request.getResumptionToken() != null) {
        oai = responseHelper.buildOaipmhResponseWithErrors(request, BAD_RESUMPTION_TOKEN, RESUMPTION_TOKEN_FORMAT_ERROR);
        future.complete(responseHelper.buildFailureResponse(oai, request));
        return future;
      }
      oai = responseHelper.buildBaseOaipmhResponse(request).withListSets(new ListSetsType()
        .withSets(getSupportedSetTypes()));
      future.complete(responseHelper.buildSuccessResponse(oai));
    } catch (Exception e) {
      logger.error("Error happened while processing ListSets verb request", e);
      future.completeExceptionally(e);
    }
    return future;
  }
}
