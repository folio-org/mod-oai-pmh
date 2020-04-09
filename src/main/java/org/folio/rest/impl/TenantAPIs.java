package org.folio.rest.impl;

import static java.lang.String.format;
import static me.escoffier.vertx.completablefuture.VertxCompletableFuture.supplyAsync;
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
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;

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
  private static final int CONFIG_JSON_BODY = 0;

  @Override
  public void postTenant(final TenantAttributes entity, final Map<String, String> headers,
      final Handler<AsyncResult<Response>> handlers, final Context context) {
    VertxCompletableFuture<Response> future = new VertxCompletableFuture<>(context);
    loadConfigData(headers, context).thenAccept(v -> {
      Response successResponse = buildResponse(HttpStatus.SC_NO_CONTENT, EMPTY);
      future.complete(successResponse);
      handlers.handle(Future.succeededFuture(successResponse));
    })
      .exceptionally(throwable -> {
        Response failureResponse = buildResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR, throwable.getMessage());
        handleException(future, throwable);
        handlers.handle(Future.succeededFuture(failureResponse));
        return null;
      });
  }

  private CompletableFuture<Void> loadConfigData(Map<String, String> headers, Context context) {
    Set<String> configs = new HashSet<>(Arrays.asList(BEHAVIOR, GENERAL, TECHNICAL));

    String okapiUrl = headers.get(X_OKAPI_URL);
    String tenant = headers.get(X_OKAPI_TENANT);

    HttpClientInterface httpClient = HttpClientFactory.getHttpClient(okapiUrl, tenant, true);
    List<CompletableFuture<Void>> completableFutures = new ArrayList<>();

    configs.forEach(config -> completableFutures.add(requestConfig(context, httpClient, headers, config)
      .thenCompose(configPair -> postConfigIfAbsent(context, httpClient, headers, configPair))
      .thenAccept(configEntry -> populateSystemProperties(context, configEntry))));

    return VertxCompletableFuture.allOf(context, completableFutures.toArray(new CompletableFuture[0]));
  }

  private CompletableFuture<Map.Entry<String, JsonObject>> requestConfig(Context context, HttpClientInterface httpClient,
      Map<String, String> headers, String configName) {
    try {
      return httpClient.request(getConfigUrl(configName), headers)
        .thenCompose(response -> supplyAsync(context, () -> new HashMap.SimpleImmutableEntry<>(configName, response.getBody())));
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

  private CompletableFuture<Map.Entry<String, JsonObject>> postConfigIfAbsent(Context context, HttpClientInterface httpClient,
      Map<String, String> headers, Map.Entry<String, JsonObject> configPair) {
    JsonObject config = configPair.getValue();
    JsonArray configs = config.getJsonArray(CONFIGS);
    if (configs.isEmpty()) {
      JsonObject configToPost = getJsonConfigFromResource(configPair.getKey());
      try {
        headers.put(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.APPLICATION_JSON.toString());
        return httpClient.request(HttpMethod.POST, configToPost, MOD_CONFIGURATION_ENTRIES_PATH, headers)
          .thenCompose(
              response -> supplyAsync(context, () -> new HashMap.SimpleImmutableEntry<>(configPair.getKey(), response.getBody())));
      } catch (Exception ex) {
        logger.error(format("Cannot post config. %s", ex.getMessage()));
        throw new IllegalStateException(ex);
      }
    } else {
      return CompletableFuture.completedFuture(configPair);
    }
  }

  private CompletableFuture<Map.Entry<String, JsonObject>> populateSystemProperties(Context context,
      Map.Entry<String, JsonObject> configPair) {
    return VertxCompletableFuture.supplyAsync(context, () -> {
      JsonObject config = configPair.getValue()
        .getJsonArray(CONFIGS)
        .getJsonObject(CONFIG_JSON_BODY);
      String configValue = config.getString(VALUE);
      Map<String, String> configKeyValueMap = parseConfigStringToKeyValueMap(configValue);
      Properties sysProps = System.getProperties();
      sysProps.putAll(configKeyValueMap);
      return configPair;
    });
  }

  private Map<String, String> parseConfigStringToKeyValueMap(String configValue) {
    JsonObject configKeyValueSet = new JsonObject(configValue);
    return configKeyValueSet.getMap()
      .entrySet()
      .stream()
      .collect(Collectors.toMap(entry -> PropertyNameMapper.mapFrontendKeyToServerKeyName(entry.getKey()),
          entry -> (String) entry.getValue()));
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
    return CONFIG_DIR_NAME.concat(File.separator)
      .concat(configJsonName)
      .concat(JSON_EXTENSION);
  }

  private Response buildResponse(int statusCode, String message) {
    Response.ResponseBuilder responseBuilder;
    if (StringUtils.isNotEmpty(message)) {
      responseBuilder = Response.status(statusCode)
        .header(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.TEXT_PLAIN.toString())
        .entity(message);
    } else {
      responseBuilder = Response.noContent()
        .status(statusCode);
    }
    return responseBuilder.build();
  }

  private void handleException(CompletableFuture<Response> future, Throwable throwable) {
    logger.error(format("Cannot enable module: %s", throwable.getMessage()));
    future.completeExceptionally(throwable);
  }

}
