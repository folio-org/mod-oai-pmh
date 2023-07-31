package org.folio.oaipmh.dao.impl;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.dao.ViewsDao;
import org.springframework.stereotype.Repository;

@Repository
public class ViewsDaoImpl implements ViewsDao {

  private PostgresClientFactory postgresClientFactory;

  public ViewsDaoImpl(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  public Future<JsonArray> query(String query, String tenantId) {
    return postgresClientFactory.getClient(tenantId).query(query).execute()
      .map(this::rowSetToString);
  }

  private JsonArray rowSetToString(RowSet<Row> rowSet) {
    JsonArray array = new JsonArray();
    rowSet.forEach(row -> {
      array.add(row.toJson());
    });
    return array;
  }
}
