package org.folio.oaipmh.service;

import java.util.List;

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

}
