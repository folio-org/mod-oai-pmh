package org.folio.oaipmh.config;

import javax.annotation.PostConstruct;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.service.ConfigurationService;
import org.springframework.stereotype.Component;

@Component
public class ConfigurationInitializer {

  private final ConfigurationService configurationService;

  public ConfigurationInitializer(ConfigurationService configurationService) {
    this.configurationService = configurationService;
  }

  @PostConstruct
  public void initialize() {
    RepositoryConfigurationUtil.setConfigurationService(configurationService);
  }
}
