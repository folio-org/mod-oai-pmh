package org.folio.oaipmh.service.impl;

import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;

import java.util.Collections;
import java.util.List;

import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.service.InstancesService;
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

}
