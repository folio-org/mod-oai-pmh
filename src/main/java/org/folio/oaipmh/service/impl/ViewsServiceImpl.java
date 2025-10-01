package org.folio.oaipmh.service.impl;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.dao.ViewsDao;
import org.folio.oaipmh.service.ViewsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ViewsServiceImpl implements ViewsService {

  private static final Logger logger = LogManager.getLogger(ViewsServiceImpl.class);

  @Autowired
  private ViewsDao viewsDao;

  @Override
  public Future<JsonArray> query(String query, String tenantId) {
    long t1 = System.nanoTime();
    return viewsDao.query(query, tenantId).onComplete(hand ->
        logger.info("Total time for query: {} sec",
            (System.nanoTime() - t1) / 1_000_000_000));
  }
}
