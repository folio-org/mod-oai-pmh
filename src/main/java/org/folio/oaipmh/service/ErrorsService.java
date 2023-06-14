package org.folio.oaipmh.service;

import io.vertx.core.Future;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;

public interface ErrorsService {

  void logLocally(String tenantId, String requestId, String instanceId, String errorMsg);

  Future<RequestMetadataLb> saveErrorsAndUpdateRequestMetadata(String tenantId, String requestId, RequestMetadataLb requestMetadata);
}
