package org.folio.oaipmh.service.impl;

import org.folio.oaipmh.service.ConsortiaService;
import org.springframework.stereotype.Service;

@Service
public class ConsortiaServiceImpl implements ConsortiaService {
  @Override
  public String getCentralTenantId() {
    return "MOBIUS";
  }

//  public String getCentralTenantId() {
//    WebClientProvider.getWebClient().getAbs()
//  }
}
