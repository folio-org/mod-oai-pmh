package org.folio.oaipmh.helpers.resource;

import static java.lang.String.format;

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

public class ResourceHelper {

  private static final Logger logger = LoggerFactory.getLogger(ResourceHelper.class);

  private static final String JSON_EXTENSION = ".json";
  private static final String VALUE = "value";

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

  public Map<String, String> getConfigKeyValueMapFromJsonConfigEntry(JsonObject configurationEntry) {
    JsonObject configKeyValueSet = new JsonObject(configurationEntry.getString(VALUE));
    return configKeyValueSet.getMap()
      .entrySet()
      .stream()
      .collect(Collectors.toMap(entry -> PropertyNameMapper.mapFrontendKeyToServerKey(entry.getKey()), entry -> entry.getValue().toString()));
  }

}
