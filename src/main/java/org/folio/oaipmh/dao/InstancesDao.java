package org.folio.oaipmh.dao;

import java.util.List;

import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;

import io.vertx.core.Future;

public interface InstancesDao {

  /**
   * Returns request ids which last updated date is 'expirationPeriodInSeconds' seconds less than the current date.
   */
  Future<List<String>> getExpiredRequestIds(String tenantId, int expirationPeriodInSeconds);

  /**
   * Saves specified request metadata. Entity must contain request id, in opposite case IllegalStateException will be thrown.
   */
  Future<RequestMetadataLb> saveRequestMetadata(RequestMetadataLb requestMetadata, String tenantId);

  /**
   * Updates request metadata by request id.
   */
  Future<RequestMetadataLb> updateRequestMetadataByRequestId(String requestId, RequestMetadataLb requestMetadataLb,
      String tenantId);

  /**
   * Deletes request metadata by request id. Due to foreign key constraint all instances with such request id will be deleted as
   * well.
   */
  Future<Boolean> deleteRequestMetadataByRequestId(String requestId, String tenantId);

  /**
   * Deletes batch of instances by provided ids list.
   */
  Future<Boolean> deleteInstancesById(List<String> instIds, String tenantId);

  /**
   * Saves batch of instances.
   */
  Future<Void> saveInstances(List<Instances> instances, String tenantId);

  /**
   * Retrieves instances by offset and limit.
   */
  Future<List<Instances>> getInstancesList(int offset, int limit, String tenantId);
}
