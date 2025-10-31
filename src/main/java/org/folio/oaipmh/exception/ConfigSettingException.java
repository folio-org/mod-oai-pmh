package org.folio.oaipmh.exception;

public class ConfigSettingException extends RuntimeException {
  public ConfigSettingException(String configName) {
    super(String.format("Configuration with name '%s' already exists", configName));
  }

}
