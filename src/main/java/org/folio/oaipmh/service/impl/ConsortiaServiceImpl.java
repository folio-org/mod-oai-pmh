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
    if (!centralTenantIds.isEmpty()) {
      var centralTenantId = centralTenantIds.getJsonObject(0).getString("centralTenantId");
      if (centralTenantId.equals(request.getTenant())) {
        logger.error("Current tenant is central");
      }
      return centralTenantId;
    }
    logger.info("No central tenant found");
    return "";
  }
}
