package org.folio.oaipmh.service.impl;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import org.folio.oaipmh.dao.ViewsDao;
import org.folio.oaipmh.service.ViewsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ViewsServiceImpl implements ViewsService {

  @Autowired
  private ViewsDao viewsDao;

  @Override
  public Future<JsonArray> query(String query, String tenantId) {
    return viewsDao.query(query, tenantId);
  }
}
