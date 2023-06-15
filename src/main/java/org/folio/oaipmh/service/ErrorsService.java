package org.folio.oaipmh.service;

import io.vertx.core.Future;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;

public interface ErrorsService {

  void log(String tenantId, String requestId, String instanceId, String errorMsg);

  Future<RequestMetadataLb> saveErrorsAndUpdateRequestMetadata(String tenantId, String requestId);

  Future<Boolean> deleteErrorsByRequestId(String tenantId, String requestId);
}
