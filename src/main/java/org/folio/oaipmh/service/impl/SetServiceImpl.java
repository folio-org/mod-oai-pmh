package org.folio.oaipmh.service.impl;

import org.folio.oaipmh.dao.SetDao;
import org.folio.oaipmh.service.SetService;
import org.folio.rest.jaxrs.model.FolioSet;
import org.folio.rest.jaxrs.model.FolioSetCollection;
import org.springframework.stereotype.Service;

import io.vertx.core.Future;

@Service
public class SetServiceImpl implements SetService {

  private final SetDao setDao;

  public SetServiceImpl(final SetDao setDao) {
    this.setDao = setDao;
  }

  @Override
  public Future<FolioSet> getSetById(String id, String tenantId) {
    return setDao.getSetById(id, tenantId);
  }

  @Override
  public Future<FolioSet> updateSetById(String id, FolioSet entry, String tenantId, String userId) {
    return setDao.updateSetById(id, entry, tenantId, userId);
  }

  @Override
  public Future<FolioSet> saveSet(FolioSet entry, String tenantId, String userId) {
    return setDao.saveSet(entry, tenantId, userId);
  }

  @Override
  public Future<Boolean> deleteSetById(String id, String tenantId) {
    return setDao.deleteSetById(id, tenantId);
  }

  @Override
  public Future<FolioSetCollection> getSetList(int offset, int limit, String tenantId) {
    return setDao.getSetList(offset, limit, tenantId);
  }
}
