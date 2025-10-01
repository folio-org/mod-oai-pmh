package org.folio.oaipmh.dao;

import io.vertx.core.Future;
import java.util.List;
import org.folio.rest.jooq.tables.pojos.Errors;

public interface ErrorsDao {

  Future<Errors> saveErrors(Errors errors, String tenantId);

  Future<List<Errors>> getErrorsList(String requestId, String tenantId);

  Future<Boolean> deleteErrorsByRequestId(String requestId, String tenantId);
}
