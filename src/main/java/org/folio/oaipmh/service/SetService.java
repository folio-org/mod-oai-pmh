package org.folio.oaipmh.service;

import org.folio.rest.jaxrs.model.SetItem;

import io.vertx.core.Future;

public interface SetService {

  Future<SetItem> getSetById(String id, String tenantId);

  Future<SetItem> updateSetById(String id, SetItem entry, String tenantId, String userId);

  Future<SetItem> saveSet(SetItem entry, String tenantId, String userId);

  Future<Boolean> deleteSetById(String id, String tenantId);

}
