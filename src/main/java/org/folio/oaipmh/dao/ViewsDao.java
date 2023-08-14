package org.folio.oaipmh.dao;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;

public interface ViewsDao {
  Future<JsonArray> query(String query, String tenantId);
}
