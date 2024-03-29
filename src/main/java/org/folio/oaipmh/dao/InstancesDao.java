package org.folio.oaipmh.dao;

import java.time.OffsetDateTime;
import java.util.List;

import org.folio.oaipmh.domain.StatisticsHolder;
import org.folio.rest.jaxrs.model.RequestMetadataCollection;
import org.folio.rest.jaxrs.model.UuidCollection;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;

import io.vertx.core.Future;

public interface InstancesDao {

  /**
   * Returns request ids which last updated date is 'expirationPeriodInSeconds' seconds less than the current date.
   */
  Future<List<String>> getExpiredRequestIds(String tenantId, long expirationPeriodInSeconds);

  Future<RequestMetadataLb> getRequestMetadataByRequestId(String requestId, String tenantId);

  /**
   * Returns request metadata collection
   */
  Future<RequestMetadataCollection> getRequestMetadataCollection(int offset, int limit, String tenantId);

  Future<UuidCollection> getFailedToSaveInstancesIdsCollection(String requestId, int offset, int limit, String tenantId);

  Future<UuidCollection> getSkippedInstancesIdsCollection(String requestId, int offset, int limit, String tenantId);

  Future<UuidCollection> getFailedInstancesIdsCollection(String requestId, int offset, int limit, String tenantId);

  Future<UuidCollection> getSuppressedInstancesIdsCollection(String requestId, int offset, int limit, String tenantId);

  /**
   * Saves specified request metadata. Entity must contain request id, in opposite case IllegalStateException will be thrown.
   */
  Future<RequestMetadataLb> saveRequestMetadata(RequestMetadataLb requestMetadata, String tenantId);

  /**
   * Updates request metadata update date column by request id.
   */
  Future<RequestMetadataLb> updateRequestUpdatedDateAndStatistics(String requestId, OffsetDateTime lastUpdatedDate, StatisticsHolder holder, String tenantId);

  /**
   * @deprecated due to issue with VertX re-usage issue
   */
  @Deprecated(since = "3.7.2", forRemoval = true)
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
   * Retrieves instances by limit, source and request id.
   */
  Future<List<Instances>> getInstancesList(int limit, String requestId, String tenantId);

  Future<RequestMetadataLb> updateRequestMetadataByPathToError(String requestId, String tenantId, String pathToErrorFile);

  Future<RequestMetadataLb> updateRequestMetadataByLinkToError(String requestId, String tenantId, String linkToError);

  Future<List<String>> getRequestMetadataIdsByStartedDateAndExistsByPathToErrorFileInS3(String tenantId, OffsetDateTime date);
}
