package org.folio.oaipmh.service.impl;

import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.service.InstancesService;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.vertx.core.Future;
import io.vertx.core.Promise;

@Service
public class InstancesServiceImpl implements InstancesService {

  protected final Logger logger = LogManager.getLogger(getClass());

  private InstancesDao instancesDao;

  public InstancesServiceImpl(InstancesDao instancesDao) {
    this.instancesDao = instancesDao;
  }

  @Override
  public Future<List<String>> cleanExpiredInstances(String tenantId, int expirationTimeSeconds) {
    Promise<List<String>> promise = Promise.promise();
    instancesDao.getExpiredRequestIds(tenantId, expirationTimeSeconds)
      .onSuccess(ids -> {
        List<Future> futures = new ArrayList<>();
        if (isNotEmpty(ids)) {
          ids.forEach(id -> futures.add(instancesDao.deleteRequestMetadataByRequestId(id, tenantId)));
          GenericCompositeFuture.all(futures)
            .onSuccess(v -> promise.complete(ids))
            .onFailure(throwable -> {
              logger.error("Error occurred during deleting instances by request ids: {}", ids, throwable);
              promise.fail(throwable);
            });
        } else {
          promise.complete(Collections.emptyList());
        }
      })
      .onFailure(th -> {
        logger.error(th.getMessage());
        promise.fail(th);
      });
    return promise.future();
  }

  @Override
  public Future<RequestMetadataLb> getRequestMetadataByRequestId(String requestId, String tenantId) {
    return instancesDao.getRequestMetadataByRequestId(requestId, tenantId);
  }

  @Override
  public Future<RequestMetadataLb> saveRequestMetadata(RequestMetadataLb requestMetadata, String tenantId) {
    return instancesDao.saveRequestMetadata(requestMetadata, tenantId);
  }

  @Override
  public Future<RequestMetadataLb> updateRequestUpdatedDate(String requestId, OffsetDateTime lastUpdatedDate, String tenantId) {
    return instancesDao.updateRequestUpdatedDate(requestId, lastUpdatedDate, tenantId);
  }

  @Override
  public Future<RequestMetadataLb> updateRequestStreamEnded(String requestId, boolean isStreamEnded, String tenantId) {
    return instancesDao.updateRequestStreamEnded(requestId, isStreamEnded, tenantId);
  }

  @Override
  public Future<Boolean> deleteRequestMetadataByRequestId(String requestId, String tenantId) {
    return instancesDao.deleteRequestMetadataByRequestId(requestId, tenantId);
  }

  @Override
  public Future<Boolean> deleteInstancesById(List<String> instIds, String requestId, String tenantId) {
    return instancesDao.deleteInstancesById(instIds, requestId, tenantId);
  }

  @Override
  public Future<Void> saveInstances(List<Instances> instances, String tenantId) {
    return instancesDao.saveInstances(instances, tenantId);
  }

  @Override
  public Future<List<Instances>> getInstancesList(int limit, String requestId, String tenantId) {
    return instancesDao.getInstancesList(limit, requestId, tenantId);
  }

  @Autowired
  public InstancesDao setInstancesDao() {
    return instancesDao;
  }
}
