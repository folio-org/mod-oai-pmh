package org.folio.oaipmh.service.impl;

import java.util.Optional;

import org.folio.oaipmh.dao.SetDao;
import org.folio.oaipmh.service.SetService;
import org.folio.rest.jaxrs.model.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@Service
public class SetServiceImpl implements SetService {

  private static final Logger logger = LoggerFactory.getLogger(SetServiceImpl.class);

  private final SetDao setDao;

  public SetServiceImpl(final SetDao setDao) {
    logger.info("SetServiceImpl constructor start");
    this.setDao = setDao;
    logger.info("SetServiceImpl constructor finish. SetDao - {}", this.setDao);
  }

  @Override
  public Future<Optional<Set>> getSetById(String id, String tenantId) {
    return setDao.getSetById(id, tenantId);
  }

  @Override
  public Future<Set> updateSetById(String id, Set entry, String tenantId) {
    return setDao.updateSetById(id, entry, tenantId);
  }

  @Override
  public Future<Set> saveSet(Set entry, String tenantId) {
    return setDao.saveSet(entry, tenantId);
  }

  @Override
  public Future<Boolean> deleteSetById(String id, String tenantId) {
    return setDao.deleteSetById(id, tenantId);
  }

}
