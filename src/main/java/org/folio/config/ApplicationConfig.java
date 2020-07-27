package org.folio.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
  "org.folio.rest.impl",
  "org.folio.oaipmh.helpers.configuration",
  "org.folio.oaipmh.dao",
  "org.folio.oaipmh.service"})
public class ApplicationConfig {
}
