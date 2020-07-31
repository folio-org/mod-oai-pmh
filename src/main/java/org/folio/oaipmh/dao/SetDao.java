package org.folio.oaipmh.dao;

import org.folio.rest.jaxrs.model.Set;
import org.folio.rest.jaxrs.model.SetCollection;

import io.vertx.core.Future;

public interface SetDao {

  Future<Set> getSetById(String id, String tenantId);

  Future<Set> updateSetById(String id, Set entry, String tenantId, String userId);

  Future<Set> saveSet(Set entry, String tenantId, String userId);

  Future<Boolean> deleteSetById(String id, String tenantId);

  Future<SetCollection> getSetList(int offset, int limit, String tenantId);

}
