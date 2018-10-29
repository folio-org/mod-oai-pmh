package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import org.folio.oaipmh.Request;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for all OAI-PMH verbs business logic implementations.
 */
public interface VerbHelper {

  String FROM_PARAM = "from";
  String UNTIL_PARAM = "until";

  String REPOSITORY_BASE_URL = "repository.baseURL";
  String IDENTIFIER_PREFIX = "identifierPrefix";

  /**
   * Performs verb specific business logic.
   *
   * @param request the OAI-PMH request
   * @param ctx the context
   * @return CompletableFuture containing OAI-PMH response string representation.
   */
  CompletableFuture<javax.ws.rs.core.Response> handle(Request request, Context ctx);
}
