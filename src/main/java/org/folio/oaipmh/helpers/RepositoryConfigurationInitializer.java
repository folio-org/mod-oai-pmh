package org.folio.oaipmh.helpers;

import javax.annotation.PostConstruct;
import org.folio.oaipmh.service.ConfigurationSettingsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Initializer component that sets up the ConfigurationSettingsService
 * for RepositoryConfigurationUtil at application startup.
 */
@Component
public class RepositoryConfigurationInitializer {

  private final ConfigurationSettingsService configurationSettingsService;

  @Autowired
  public RepositoryConfigurationInitializer(
      ConfigurationSettingsService configurationSettingsService) {
    this.configurationSettingsService = configurationSettingsService;
  }

  @PostConstruct
  public void init() {
    RepositoryConfigurationUtil.setConfigurationSettingsService(configurationSettingsService);
  }
}
