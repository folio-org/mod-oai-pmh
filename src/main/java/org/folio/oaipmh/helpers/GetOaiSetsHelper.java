package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;
import org.apache.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseHelper;
import org.folio.rest.jaxrs.resource.Oai.GetOaiSetsResponse;
import org.openarchives.oai._2.ListSetsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.VerbType;

import javax.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;

import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.VerbType.LIST_SETS;

/**
 * Helper class that contains business logic for retrieving OAI-PMH set structure of a repository.
 */
public class GetOaiSetsHelper extends AbstractHelper {
  private static final Logger logger = Logger.getLogger(GetOaiSetsHelper.class);

  @Override
  public CompletableFuture<Response> handle(Request request, Context ctx) {
    CompletableFuture<Response> future = new VertxCompletableFuture<>(ctx);
    try {
      OAIPMH oai = buildBaseResponse(request);

      if (request.getResumptionToken() != null) {
        oai.withErrors(new OAIPMHerrorType()
          .withCode(BAD_RESUMPTION_TOKEN).withValue(RESUMPTION_TOKEN_FORMAT_ERROR));
        future.complete(GetOaiSetsResponse
          .respond400WithApplicationXml(ResponseHelper.getInstance().writeToString(oai)));
        return future;
      }

      oai.withListSets(new ListSetsType()
        .withSets(getSupportedSetTypes()));
      future.complete(GetOaiSetsResponse
        .respond200WithApplicationXml(ResponseHelper.getInstance().writeToString(oai)));
    } catch (Exception e) {
      logger.error("Error happened while processing ListSets verb request", e);
      future.completeExceptionally(e);
    }

    return future;
  }

  @Override
  protected VerbType getVerb() {
    return LIST_SETS;
  }
}
