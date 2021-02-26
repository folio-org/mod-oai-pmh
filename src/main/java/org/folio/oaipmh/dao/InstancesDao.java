package org.folio.oaipmh.dao;

import java.time.OffsetDateTime;
import java.util.List;

import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;

import io.vertx.core.Future;

public interface InstancesDao {

  /**
   * Returns request ids which last updated date is 'expirationPeriodInSeconds' seconds less than the current date.
   */
  Future<List<String>> getExpiredRequestIds(String tenantId, int expirationPeriodInSeconds);

  Future<RequestMetadataLb> getRequestMetadataByRequestId(String requestId, String tenantId);

  /**
   * Saves specified request metadata. Entity must contain request id, in opposite case IllegalStateException will be thrown.
   */
  Future<RequestMetadataLb> saveRequestMetadata(RequestMetadataLb requestMetadata, String tenantId);

  /**
   * Updates request metadata update date column by request id.
   */
  Future<RequestMetadataLb> updateRequestUpdatedDate(String requestId, OffsetDateTime lastUpdatedDate, String tenantId);

  /**
   * Updates request metadata stream ended column by request id.
   */
  Future<RequestMetadataLb> updateRequestStreamEnded(String requestId, boolean isStreamEnded, String tenantId);

  /**
   * Deletes request metadata by request id. Due to foreign key constraint all instances with such request id will be deleted as
   * well.
   */
  Future<Boolean> deleteRequestMetadataByRequestId(String requestId, String tenantId);

  /**
   * Deletes batch of instances by provided ids list and request id.
   */
  Future<Boolean> deleteInstancesById(List<String> instIds, String requestId, String tenantId);

  /**
   * Saves batch of instances.
   */
  Future<Void> saveInstances(List<Instances> instances, String tenantId);

  /**
   * Retrieves instances by limit and request id.
   */
  Future<List<Instances>> getInstancesList(int limit, String requestId, String tenantId);

  /**
   * Retrieves instances which have PK id value >= id by limit and request id.
   */
  Future<List<Instances>> getInstancesList(int limit, String requestId, int id, String tenantId);

}
