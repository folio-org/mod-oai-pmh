package org.folio.oaipmh.service;

import org.folio.rest.jaxrs.model.SetItem;
import org.folio.rest.jaxrs.model.SetItemCollection;

import io.vertx.core.Future;

public interface SetService {

  Future<SetItem> getSetById(String id, String tenantId);

  Future<SetItem> updateSetById(String id, SetItem entry, String tenantId, String userId);

  Future<SetItem> saveSet(SetItem entry, String tenantId, String userId);

  Future<Boolean> deleteSetById(String id, String tenantId);

  Future<SetItemCollection> getSetList(int offset, int limit, String tenantId);

}
