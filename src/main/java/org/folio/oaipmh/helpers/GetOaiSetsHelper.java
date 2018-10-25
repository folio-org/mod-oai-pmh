package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;
import org.apache.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseHelper;
import org.openarchives.oai._2.ListSetsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.SetType;
import org.openarchives.oai._2.VerbType;

import java.util.concurrent.CompletableFuture;

/**
 * Helper class that contains business logic for retrieving OAI-PMH set structure of a repository.
 */
public class GetOaiSetsHelper extends AbstractHelper {
  private static final Logger logger = Logger.getLogger(GetOaiSetsHelper.class);

  @Override
  public CompletableFuture<String> handle(Request request, Context ctx) {
    CompletableFuture<String> future = new VertxCompletableFuture<>(ctx);
    try {
      OAIPMH oai = buildBaseResponse(request.getOaiRequest().withVerb(VerbType.LIST_SETS))
        .withListSets(new ListSetsType()
          .withSets(new SetType()
            .withSetSpec("all")
            .withSetName("All records")));

      String response = ResponseHelper.getInstance().writeToString(oai);
      future.complete(response);
    } catch (Exception e) {
      logger.error("Error happened while processing ListSets verb request", e);
      future.completeExceptionally(e);
    }

    return future;
  }
}
