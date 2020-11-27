package org.folio.oaipmh.service;

import java.util.List;

import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.springframework.stereotype.Service;

import io.vertx.core.Future;

@Service
public interface InstancesService {

  /**
   * Performs cleaning of instances which place at DB more then 'expirationTimeSeconds' seconds.
   *
   * @param tenantId - tenant id
   * @return request ids associated with removed instances
   */
  Future<List<String>> cleanExpiredInstances(String tenantId, int expirationTimeSeconds);

  Future<RequestMetadataLb> saveRequestMetadata(RequestMetadataLb requestMetadata, String tenantId);

  Future<RequestMetadataLb> updateRequestMetadataByRequestId(String requestId, RequestMetadataLb requestMetadataLb, String tenantId);

  Future<Boolean> deleteRequestMetadataByRequestId(String requestId, String tenantId);

  Future<Boolean> deleteInstancesById(List<String> instIds, String tenantId);

  Future<Void> saveInstances(List<Instances> instances, String tenantId);

  Future<List<Instances>> getInstancesList(int offset, int limit, String tenantId);

}
