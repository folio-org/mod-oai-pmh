package org.folio.oaipmh.dao;

import java.util.List;

import io.vertx.core.Future;

public interface InstancesDao {

  /**
   * Removes instances which are associated with specified request ids.
   *
   * @param tenantId   - tenant id
   * @param requestIds - request ids for which instance cleaning is performed
   * @return true if some entities were removed, false if there no entities to be removed by provided request ids
   */
  Future<Boolean> deleteExpiredInstancesByRequestId(String tenantId, List<String> requestIds);

  /**
   * Returns request ids which last updated date is 'expirationPeriodInSeconds' seconds less than the current date.
   *
   * @param tenantId                  - tenant id
   * @param expirationPeriodInSeconds - expiration time in seconds
   * @return list of expired request ids
   */
  Future<List<String>> getExpiredRequestIds(String tenantId, int expirationPeriodInSeconds);

}
