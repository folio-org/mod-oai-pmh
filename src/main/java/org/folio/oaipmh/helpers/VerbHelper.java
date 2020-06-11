package org.folio.oaipmh.helpers;

import javax.ws.rs.core.Response;

import org.folio.oaipmh.Request;

import io.vertx.core.Context;
import io.vertx.core.Future;

/**
 * Interface for all OAI-PMH verbs business logic implementations.
 */
public interface VerbHelper {

  /**
   * Performs verb specific business logic.
   *
   * @param request the OAI-PMH request
   * @param ctx the context
   * @return CompletableFuture containing OAI-PMH response string representation.
   */
  Future<Response> handle(Request request, Context ctx);
}
