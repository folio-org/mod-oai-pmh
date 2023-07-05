package org.folio.oaipmh.service.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.client.ConsortiaClient;
import org.folio.oaipmh.service.ConsortiaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ConsortiaServiceImpl implements ConsortiaService {

  private static final Logger logger = LogManager.getLogger(ConsortiaServiceImpl.class);

  @Autowired
  private ConsortiaClient consortiaClient;

  @Override
  public String getCentralTenantId(Request request) {
    var centralTenantIds = consortiaClient.getUserTenants(request);
    if (centralTenantIds.size() == 1) {
      return centralTenantIds.get(0);
    } else if (centralTenantIds.size() > 1) {
      return request.getTenant();
    }
    logger.info("No central tenant found");
    return "";
  }
}
