package org.folio.oaipmh.helpers.configuration;

import static java.lang.String.format;
import static org.folio.oaipmh.Constants.VALUE;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

import org.folio.oaipmh.mappers.PropertyNameMapper;
import org.jetbrains.annotations.NotNull;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class ConfigurationHelper {

  private static final Logger logger = LoggerFactory.getLogger(ConfigurationHelper.class);

  private static final String JSON_EXTENSION = ".json";

  /**
   * Reads json file under the resource folder and maps it to {@link JsonObject}
   *
   * @param dirPath      - path to directory which holds configuration json file
   * @param jsonFileName - name of json configuration file
   * @return {@link JsonObject}
   */
  public JsonObject getJsonConfigFromResources(String dirPath, String jsonFileName) {
    String configJsonPath = buildConfigPath(dirPath, jsonFileName);
    try (InputStream is = getClass().getClassLoader()
      .getResourceAsStream(configJsonPath)) {
      if (is == null) {
        String message = format("Unable open the resource file %s", configJsonPath);
        logger.error(message);
        throw new IllegalStateException(message);
      }
      try (InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
          BufferedReader reader = new BufferedReader(isr)) {
        String config = reader.lines()
          .collect(Collectors.joining(System.lineSeparator()));
        return new JsonObject(config);
      }
    } catch (IOException ex) {
      logger.error(ex.getMessage(), ex);
      throw new IllegalStateException(ex);
    }
  }

  @NotNull
  private String buildConfigPath(String dirPath, String configJsonName) {
    return dirPath.concat(File.separator)
      .concat(configJsonName)
      .concat(JSON_EXTENSION);
  }

  /**
   * Parses configurations form string within JsonObject value field to map.
   *
   * @param configurationEntry - json configuration entry
   * @return {@link Map}
   */
  public Map<String, String> getConfigKeyValueMapFromJsonConfigEntry(JsonObject configurationEntry) {
    JsonObject configKeyValueSet = new JsonObject(configurationEntry.getString(VALUE));
    return configKeyValueSet.getMap()
      .entrySet()
      .stream()
      .collect(Collectors.toMap(entry -> PropertyNameMapper.mapFrontendKeyToServerKey(entry.getKey()), entry -> entry.getValue().toString()));
  }

}
