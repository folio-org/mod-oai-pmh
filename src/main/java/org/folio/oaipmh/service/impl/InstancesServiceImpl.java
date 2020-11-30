package org.folio.oaipmh.service.impl;

import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;

import java.util.Collections;
import java.util.List;

import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.service.InstancesService;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@Service
public class InstancesServiceImpl implements InstancesService {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  private InstancesDao instancesDao;

  public InstancesServiceImpl(InstancesDao instancesDao) {
    this.instancesDao = instancesDao;
  }

  @Override
  public Future<List<String>> cleanExpiredInstances(String tenantId, int expirationTimeSeconds) {
    Promise<List<String>> promise = Promise.promise();
    return instancesDao.getExpiredRequestIds(tenantId, expirationTimeSeconds)
      .compose(ids -> {
        if (isNotEmpty(ids)) {
          instancesDao.deleteExpiredInstancesByRequestId(tenantId, ids)
            .onSuccess(result -> promise.complete(ids))
            .onFailure(throwable -> {
              logger.error("Error occurred during deleting instances by request ids: " + ids, throwable);
              promise.fail(throwable);
            });
          return promise.future();
        } else {
          promise.complete(Collections.emptyList());
          return promise.future();
        }
      });
  }

  @Override
  public Future<RequestMetadataLb> saveRequestMetadata(RequestMetadataLb requestMetadata, String tenantId) {
    return instancesDao.saveRequestMetadata(requestMetadata, tenantId);
  }

  @Override
  public Future<RequestMetadataLb> updateRequestMetadataByRequestId(String requestId, RequestMetadataLb requestMetadataLb,
      String tenantId) {
    return instancesDao.updateRequestMetadataByRequestId(requestId, requestMetadataLb, tenantId);
  }

  @Override
  public Future<Boolean> deleteRequestMetadataByRequestId(String requestId, String tenantId) {
    return instancesDao.deleteRequestMetadataByRequestId(requestId, tenantId);
  }

  @Override
  public Future<Boolean> deleteInstancesById(List<String> instIds, String tenantId) {
    return instancesDao.deleteInstancesById(instIds, tenantId);
  }

  @Override
  public Future<Void> saveInstances(List<Instances> instances, String tenantId) {
    return instancesDao.saveInstances(instances, tenantId);
  }

  @Override
  public Future<List<Instances>> getInstancesList(int offset, int limit, String tenantId) {
    return instancesDao.getInstancesList(offset, limit, tenantId);
  }

  @Autowired
  public InstancesDao setInstancesDao() {
    return instancesDao;
  }
}
