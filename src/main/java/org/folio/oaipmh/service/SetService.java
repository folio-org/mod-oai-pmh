package org.folio.oaipmh.service;

import org.folio.rest.jaxrs.model.FolioSet;
import org.folio.rest.jaxrs.model.FolioSetCollection;
import java.util.Map;

import org.folio.rest.jaxrs.model.FilteringConditionValueCollection;

import io.vertx.core.Future;

public interface SetService {

  Future<FolioSet> getSetById(String id, String tenantId);

  Future<FolioSet> updateSetById(String id, FolioSet entry, String tenantId, String userId);

  Future<FolioSet> saveSet(FolioSet entry, String tenantId, String userId);

  Future<Boolean> deleteSetById(String id, String tenantId);

  Future<FolioSetCollection> getSetList(int offset, int limit, String tenantId);

  Future<FilteringConditionValueCollection> getFilteringConditions(Map<String, String> okapiHeaders);

}
