package org.folio.oaipmh.service.impl;

import org.folio.oaipmh.dao.SetDao;
import org.folio.oaipmh.service.SetService;
import org.folio.rest.jaxrs.model.Set;
import org.folio.rest.jaxrs.model.SetCollection;
import org.springframework.stereotype.Service;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@Service
public class SetServiceImpl implements SetService {

  private static final Logger logger = LoggerFactory.getLogger(SetServiceImpl.class);

  private final SetDao setDao;

  public SetServiceImpl(final SetDao setDao) {
    this.setDao = setDao;
  }

  @Override
  public Future<Set> getSetById(String id, String tenantId) {
    return setDao.getSetById(id, tenantId);
  }

  @Override
  public Future<Set> updateSetById(String id, Set entry, String tenantId, String userId) {
    return setDao.updateSetById(id, entry, tenantId, userId);
  }

  @Override
  public Future<Set> saveSet(Set entry, String tenantId, String userId) {
    return setDao.saveSet(entry, tenantId, userId);
  }

  @Override
  public Future<Boolean> deleteSetById(String id, String tenantId) {
    return setDao.deleteSetById(id, tenantId);
  }

  @Override
  public Future<SetCollection> getSetList(int offset, int limit, String tenantId) {
    return setDao.getSetList(offset, limit, tenantId);
  }
}
