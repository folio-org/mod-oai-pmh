package org.folio.oaipmh.service;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;

public interface ViewsService {

  Future<JsonArray> query(String query, String tenantId);
}
