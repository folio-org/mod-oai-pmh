package org.folio.rest.impl;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.folio.oaipmh.ResponseHelper;
import org.folio.oaipmh.helpers.storage.CQLQueryBuilder;
import org.folio.oaipmh.mappers.PropertyNameMapper;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.client.HttpClientFactory;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.jetbrains.annotations.NotNull;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class TenantAPIs extends TenantAPI {
  private final Logger logger = LoggerFactory.getLogger(TenantAPIs.class);

  private static final String X_OKAPI_URL = "x-okapi-url";
  private static final String X_OKAPI_TENANT = "x-okapi-tenant";
  private static final String MOD_CONFIGURATION_ENTRIES_PATH = "/configurations/entries";
  private static final String BEHAVIOR = "behavior";
  private static final String GENERAL = "general";
  private static final String TECHNICAL = "technical";
  private static final String CONFIG_NAME = "configName";
  private static final String ENABLED = "enabled";
  private static final String CONFIGS = "configs";
  private static final String CONFIG_DIR_NAME = "config";
  private static final String JSON_EXTENSION = ".json";
  private static final String VALUE = "value";
  private static final int KEY_POSITION = 0;
  private static final int VALUE_POSITION = 1;
  private static final int CONFIG_JSON_BODY = 0;

  @Override
  public void postTenant(final TenantAttributes entity, final Map<String, String> headers,
      final Handler<AsyncResult<Response>> handlers, final Context context) {
    Set<String> configNames = new HashSet<>(Arrays.asList(BEHAVIOR, GENERAL, TECHNICAL));
    loadConfigData(headers, configNames).thenAccept(v -> handlers.handle(Future.succeededFuture(buildResponse(HttpStatus.SC_OK, EMPTY))))
      .exceptionally(throwable -> {
        handlers.handle(Future.succeededFuture(buildResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR, throwable.getMessage())));
        return null;
      });
  }

  private CompletableFuture<Void> loadConfigData(Map<String, String> headers, Set<String> configs) {
    String okapiUrl = headers.get(X_OKAPI_URL);
    String tenant = headers.get(X_OKAPI_TENANT);

    HttpClientInterface httpClient = HttpClientFactory.getHttpClient(okapiUrl, tenant, true);
    List<CompletableFuture<Void>> completableFutures = new ArrayList<>();

    configs.forEach(config -> completableFutures
      .add(requestConfig(httpClient, headers, config).thenCompose(configPair -> postConfigIfAbsent(httpClient, headers, configPair))
        .thenAccept(this::populateSystemProperties)));
    return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]));
  }

  private CompletableFuture<Map.Entry<String, JsonObject>> requestConfig(HttpClientInterface httpClient,
      Map<String, String> headers, String configName) {
    try {
      return httpClient.request(getConfigUrl(configName), headers)
        .thenApply(response -> new HashMap.SimpleImmutableEntry<>(configName, response.getBody()));
    } catch (Exception ex) {
      logger.error(format("Cannot get config with configName - %s. %s", configName, ex.getMessage()));
      throw new IllegalStateException(ex);
    }
  }

  private String getConfigUrl(String configName) throws UnsupportedEncodingException {
    CQLQueryBuilder queryBuilder = new CQLQueryBuilder();
    queryBuilder.addStrictCriteria(CONFIG_NAME, configName)
      .and();
    queryBuilder.addStrictCriteria(ENABLED, Boolean.TRUE.toString());
    return MOD_CONFIGURATION_ENTRIES_PATH.concat(queryBuilder.build());
  }

  private CompletableFuture<Map.Entry<String, JsonObject>> postConfigIfAbsent(HttpClientInterface httpClient,
      Map<String, String> headers, Map.Entry<String, JsonObject> configPair) {
    JsonObject config = configPair.getValue();
    JsonArray configs = config.getJsonArray(CONFIGS);
    if (configs.isEmpty()) {
      JsonObject configToPost = getJsonConfigFromResource(configPair.getKey());
      try {
        headers.put(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.APPLICATION_JSON.toString());
        return httpClient.request(HttpMethod.POST, configToPost, MOD_CONFIGURATION_ENTRIES_PATH, headers)
          .thenApply(response -> new HashMap.SimpleImmutableEntry<>(configPair.getKey(), response.getBody()));
      } catch (Exception ex) {
        logger.error(format("Cannot post config. %s", ex.getMessage()));
        throw new IllegalStateException(ex);
      }
    } else {
      return CompletableFuture.completedFuture(configPair);
    }
  }

  private CompletableFuture<Map.Entry<String, JsonObject>> populateSystemProperties(Map.Entry<String, JsonObject> configPair) {
    return CompletableFuture.supplyAsync(() -> {
      JsonObject config = configPair.getValue()
        .getJsonArray(CONFIGS)
        .getJsonObject(CONFIG_JSON_BODY);
      String configValue = config.getString(VALUE);
      Map<String, String> configKeyValueMap = parseConfigToKeyValueMap(configValue);
      Properties sysProps = System.getProperties();
      sysProps.putAll(configKeyValueMap);
      return configPair;
    });
  }

  /**
   * Parses the string of configs key value to java.util.Map
   * Example: {"key1":"value1","key2":"va,lue2","key3","val,:ue3"} will be parsed
   * to map: key1 - value1 ; key2 - va,lue2 ; key3 - val,:ue3
   *
   * @param configValue - string that consist config key value pairs separated by comma
   * @return Map
   */
  private Map<String, String> parseConfigToKeyValueMap(String configValue) {
    Map<String, String> configKeyValueMap = new HashMap<>();
    configValue = configValue.replaceAll("\\{","")
      .replaceAll("}","");
    List<String> keyValuePairs = Arrays.asList(configValue.split("\",\""));
    keyValuePairs.forEach(keyValueString -> {
      List<String> pair = Arrays.stream(keyValueString.split("\":\""))
        .map(str->str.replaceAll("\"",""))
        .collect(Collectors.toList());
      configKeyValueMap.put(PropertyNameMapper.mapFrontendKeyToServerKeyName(pair.get(KEY_POSITION)), pair.get(VALUE_POSITION));
    });
    return configKeyValueMap;
  }

  private JsonObject getJsonConfigFromResource(String configJsonName) {
    String configJsonPath = buildConfigPath(configJsonName);
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
    } catch (IOException ex) {
      logger.error(ex.getMessage(), ex);
      throw new IllegalStateException(ex);
    }
  }

  @NotNull
  private String buildConfigPath(final String configJsonName) {
    return CONFIG_DIR_NAME
      .concat(File.separator)
      .concat(configJsonName)
      .concat(JSON_EXTENSION);
  }

  private Response buildResponse(int statusCode, String message) {
    Response.ResponseBuilder responseBuilder;
    if(StringUtils.isNotEmpty(message)){
      responseBuilder = Response.status(statusCode)
        .header(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.TEXT_PLAIN.toString())
        .entity(message);
    }else {
      responseBuilder = Response.noContent().status(statusCode);
    }
    return responseBuilder.build();
  }

}
