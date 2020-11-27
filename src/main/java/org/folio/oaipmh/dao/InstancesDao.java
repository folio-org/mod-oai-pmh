package org.folio.oaipmh.dao;

import java.util.List;

import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;

import io.vertx.core.Future;

public interface InstancesDao {

  /**
   * Returns request ids which last updated date is 'expirationPeriodInSeconds' seconds less than the current date.
   *
   * @param tenantId                  - tenant id
   * @param expirationPeriodInSeconds - expiration time in seconds
   * @return list of expired request ids
   */
  Future<List<String>> getExpiredRequestIds(String tenantId, int expirationPeriodInSeconds);

  Future<RequestMetadataLb> saveRequestMetadata(RequestMetadataLb requestMetadata, String tenantId);

  Future<RequestMetadataLb> updateRequestMetadataByRequestId(String requestId, RequestMetadataLb requestMetadataLb, String tenantId);

  Future<Boolean> deleteRequestMetadataByRequestId(String requestId, String tenantId);

  /**
   * Removes instances which are associated with specified request ids.
   *
   * @param tenantId   - tenant id
   * @param requestIds - request ids for which instance cleaning is performed
   * @return true if some entities were removed, false if there no entities to be removed by provided request ids
   */
  Future<Boolean> deleteExpiredInstancesByRequestId(String tenantId, List<String> requestIds);

  Future<Boolean> deleteInstancesById(List<String> instIds, String tenantId);

  Future<Void> saveInstances(List<Instances> instances, String tenantId);

  Future<List<Instances>> getInstancesList(int offset, int limit, String tenantId);
}
