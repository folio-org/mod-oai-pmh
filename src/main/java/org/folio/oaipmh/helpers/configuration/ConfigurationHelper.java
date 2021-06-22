package org.folio.oaipmh.helpers.configuration;

import static java.lang.String.format;
import static org.folio.oaipmh.Constants.VALUE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.folio.oaipmh.mappers.PropertyNameMapper;
import org.springframework.stereotype.Component;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Is used for reading json configuration files from resources and mapping them to {@link JsonObject} instances.
 * As well allows to parse configuration keys/values from json entry to Map.
 */
@Component
public class ConfigurationHelper {

  private static final Logger logger = LogManager.getLogger(ConfigurationHelper.class);

  private static ConfigurationHelper instance;

  public ConfigurationHelper(){}

  public static ConfigurationHelper getInstance() {
    if(Objects.nonNull(instance)){
      return instance;
    }
    instance = new ConfigurationHelper();
    return instance;
  }

  /**
   * Reads json file under the resource folder and maps it to {@link JsonObject}
   *
   * @param dirPath      - path to directory which holds configuration json file
   * @param jsonFileName - name of json configuration file
   * @return {@link JsonObject}
   * @throws IllegalStateException when dirPath or jsonFileName has invalid name or doesn't exist
   */
  public JsonObject getJsonConfigFromResources(String dirPath, String jsonFileName) {
    String configJsonPath = buildConfigPath(dirPath, jsonFileName);
    try (InputStream is = getClass().getClassLoader()
      .getResourceAsStream(configJsonPath)) {
      if (is == null) {
        String message = format("Unable open the resource file %s", configJsonPath);
        logger.error(message);
        throw new IllegalArgumentException(message);
      }
      try (InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
          BufferedReader reader = new BufferedReader(isr)) {
        String config = reader.lines()
          .collect(Collectors.joining(System.lineSeparator()));
        return new JsonObject(config);
      }
    } catch (IOException | IllegalArgumentException ex) {
      logger.error(ex.getMessage(), ex);
      throw new IllegalStateException(ex);
    }
  }

  private String buildConfigPath(String dirPath, String configJsonName) {
    return dirPath.concat("/")
      .concat(configJsonName);
  }

  /**
   * Parses configurations from string of JsonObject value field to map.
   *
   * @param configurationEntry - json configuration entry
   * @return {@link Map}
   * @throws IllegalArgumentException when configurationEntry doesn't contain the value field or value field has incorrect structure
   */
  public Map<String, String> getConfigKeyValueMapFromJsonEntryValueField(JsonObject configurationEntry) {
    try {
      JsonObject configKeyValueSet = new JsonObject(configurationEntry.getString(VALUE));
      return configKeyValueSet.getMap()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(entry -> PropertyNameMapper.mapFrontendKeyToServerKey(entry.getKey()), entry -> entry.getValue().toString()));
    } catch (NullPointerException ex) {
      throw new IllegalArgumentException("Incorrect JsonObject. JsonObject doesn't contain the \'value\' field", ex);
    } catch (DecodeException ex) {
      throw new IllegalArgumentException(format("Incorrect JsonObject. JsonObject \'value\' field has incorrect structure: %s", ex.getMessage()), ex);
    }
  }

}
