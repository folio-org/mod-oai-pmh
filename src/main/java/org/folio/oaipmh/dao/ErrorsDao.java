package org.folio.oaipmh.dao;

import io.vertx.core.Future;
import org.folio.rest.jooq.tables.pojos.Errors;

import java.util.List;

public interface ErrorsDao {

  Future<Errors> saveErrors(Errors errors, String tenantId);

  Future<List<Errors>> getErrorsList(String requestId, String tenantId);

  Future<Boolean> deleteErrorsByRequestId(String requestId, String tenantId);
}
