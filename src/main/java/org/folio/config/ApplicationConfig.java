package org.folio.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@ComponentScan(basePackages = {
  "org.folio.rest.impl",
  "org.folio.oaipmh.helpers",
  "org.folio.oaipmh.helpers.configuration",
  "org.folio.oaipmh.helpers.referencedata",
  "org.folio.oaipmh.helpers.client",
  "org.folio.oaipmh.dao",
  "org.folio.oaipmh.service",
  "org.folio.oaipmh.validator",
  "org.folio.oaipmh.processors"})
@PropertySource("classpath:minio.properties")
public class ApplicationConfig {
}
