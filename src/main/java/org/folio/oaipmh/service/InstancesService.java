package org.folio.oaipmh.service;

import io.vertx.core.Future;
import java.time.OffsetDateTime;
import java.util.List;
import org.folio.oaipmh.domain.StatisticsHolder;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.springframework.stereotype.Service;

@Service
public interface InstancesService {

  /**
   * Performs cleaning of instances which have request id with lastUpdateDate less or equal
   * to current date - 'expirationTimeSeconds'.
   */
  Future<List<String>> cleanExpiredInstances(String tenantId, long expirationTimeSeconds);

  Future<RequestMetadataLb> getRequestMetadataByRequestId(String requestId, String tenantId);

  /**
   * Saves specified request metadata. Entity must contain request id, in opposite case
   * IllegalStateException will be thrown.
   */
  Future<RequestMetadataLb> saveRequestMetadata(RequestMetadataLb requestMetadata,
      String tenantId);

  /**
   * Updates request metadata updated date column by request id.
   */
  Future<RequestMetadataLb> updateRequestUpdatedDateAndStatistics(String requestId,
      OffsetDateTime lastUpdatedDate, StatisticsHolder holder, String tenantId);

  /**
   * Updates request metadata stream ended column by request id.
   */
  Future<RequestMetadataLb> updateRequestStreamEnded(String requestId, boolean isStreamEnded,
      String tenantId);

  /**
   * Deletes request metadata by request id. Due to foreign key constraint all instances
   * with such request id will be deleted as well.
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

  Future<RequestMetadataLb> updateRequestMetadataByPathToError(String requestId, String tenantId,
      String pathToErrorFile);

  Future<RequestMetadataLb> updateRequestMetadataByLinkToError(String requestId, String tenantId,
      String linkToError);

  Future<List<String>> getRequestMetadataIdsByStartedDateAndExistsByPathToErrorFileInS3(
      String tenantId, OffsetDateTime date);
}
