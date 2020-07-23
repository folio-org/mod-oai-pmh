package org.folio.oaipmh.dao;

import java.util.Optional;

import org.folio.rest.jaxrs.model.Set;

import io.vertx.core.Future;

public interface SetDao {

  Future<Optional<Set>> getSetById(String id, String tenantId);

  Future<Set> updateSetById(String id, Set entry, String tenantId);

  Future<Set> saveSet(Set entry, String tenantId);

  Future<Boolean> deleteSetById(String id, String tenantId);

}
