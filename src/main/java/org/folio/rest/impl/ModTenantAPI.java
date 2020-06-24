package org.folio.rest.impl;

import static java.lang.String.format;
import static me.escoffier.vertx.completablefuture.VertxCompletableFuture.supplyAsync;
import static org.folio.oaipmh.Constants.VALUE;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import javax.ws.rs.core.Response;

import org.apache.http.HttpStatus;
import org.folio.oaipmh.Constants;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.folio.oaipmh.helpers.storage.CQLQueryBuilder;
import org.folio.oaipmh.mappers.PropertyNameMapper;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.client.HttpClientFactory;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;

public class ModTenantAPI extends TenantAPI {
  private final Logger logger = LoggerFactory.getLogger(ModTenantAPI.class);

  private static final String SHOULD_NOT_UPDATE_PROPS = "shouldUpdateProps";
  private static final String X_OKAPI_URL = "x-okapi-url";
  private static final String X_OKAPI_TENANT = "x-okapi-tenant";
  private static final String MOD_CONFIGURATION_ENTRIES_URI = "/configurations/entries";
  private static final String CONFIG_NAME = "configName";
  private static final String ENABLED = "enabled";
  private static final String CONFIG_DIR_PATH = "config";
  private static final int CONFIG_JSON_BODY = 0;
  private ConfigurationHelper configurationHelper = ConfigurationHelper.getInstance();

  @Override
  public void postTenant(final TenantAttributes entity, final Map<String, String> headers,
      final Handler<AsyncResult<Response>> handlers, final Context context) {
    loadConfigData(headers, context).thenAccept(response -> handlers.handle(Future.succeededFuture(response)))
      .exceptionally(handleError(handlers));
  }

  private CompletableFuture<Response> loadConfigData(Map<String, String> headers, Context context) {
    VertxCompletableFuture<Response> future = new VertxCompletableFuture<>(context);
    List<String> configsSet = Arrays.asList("behavior","general","technical");

    String okapiUrl = headers.get(X_OKAPI_URL);
    String tenant = headers.get(X_OKAPI_TENANT);

    // TODO: HttpClientInstance occurrence. Need changes.
    HttpClientInterface httpClient = HttpClientFactory.getHttpClient(okapiUrl, tenant, true);
    List<CompletableFuture<Void>> completableFutures = new ArrayList<>();

    configsSet.forEach(config -> completableFutures.add(requestConfig(context, httpClient, headers, config)
      .thenCompose(configPair -> postConfigIfAbsent(context, httpClient, headers, configPair))
      .thenAccept(configEntry -> populateSystemProperties(context, configEntry))));

    VertxCompletableFuture.allOf(context, completableFutures.toArray(new CompletableFuture[0]))
      .thenCompose(v -> buildSuccessResponse(context))
      .thenAccept(future::complete)
      .exceptionally(throwable -> {
        handleException(future, throwable);
        return null;
      });
    return future;
  }

  // TODO: HttpClientInstance occurrence. Need changes.
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

  // TODO: CQLQueryBuilder used here. Need changes.
  private String getConfigUrl(String configName) throws UnsupportedEncodingException {
    CQLQueryBuilder queryBuilder = new CQLQueryBuilder();
    queryBuilder.addStrictCriteria(CONFIG_NAME, configName)
      .and();
    queryBuilder.addStrictCriteria(ENABLED, Boolean.TRUE.toString());
    return MOD_CONFIGURATION_ENTRIES_URI.concat(queryBuilder.build());
  }

  // TODO: HttpClientInstance occurrence. Need changes.
  private CompletableFuture<Map.Entry<String, JsonObject>> postConfigIfAbsent(Context context, HttpClientInterface httpClient,
      Map<String, String> headers, Map.Entry<String, JsonObject> configPair) {
    JsonObject config = configPair.getValue();
    JsonArray configs = config.getJsonArray(Constants.CONFIGS);
    if (configs.isEmpty()) {
      JsonObject configToPost = prepareJsonToPost(configPair.getKey().concat(".json"));
      try {
        headers.put(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.APPLICATION_JSON.toString());
        return httpClient.request(HttpMethod.POST, configToPost, MOD_CONFIGURATION_ENTRIES_URI, headers)
          .thenCompose(
              response -> supplyAsync(context, () -> new HashMap.SimpleImmutableEntry<>(configPair.getKey(), response.getBody()
                .put(SHOULD_NOT_UPDATE_PROPS, true))));
      } catch (Exception ex) {
        logger.error(format("Cannot post config. %s", ex.getMessage()));
        throw new IllegalStateException(ex);
      }
    } else {
      return CompletableFuture.completedFuture(configPair);
    }
  }

  /**
   * Substitutes default values of json configuration(within value field) with values specified through JVM. In case get request to
   * mod-configuration doesn't return config entry then default json with default configuration values will be posted but if JVM
   * property has been specified for one of it default values then such default value should be replaced with value specified via
   * JVM.
   *
   * @param jsonConfigFileName - json file with default configurations that is placed under the resource folder.
   * @return json object with replaced values with specified via JVM if they were specified
   */
  private JsonObject prepareJsonToPost(String jsonConfigFileName) {
    Properties systemProperties = System.getProperties();
    String configPath = systemProperties.getProperty("configPath", CONFIG_DIR_PATH);
    JsonObject jsonConfigEntry = configurationHelper.getJsonConfigFromResources(configPath, jsonConfigFileName);
    Map<String, String> configKeyValueMap = configurationHelper.getConfigKeyValueMapFromJsonEntryValueField(jsonConfigEntry);

    JsonObject configEntryValueField = new JsonObject();
    configKeyValueMap.forEach((key, configDefaultValue) -> {
      String possibleJvmSpecifiedValue = systemProperties.getProperty(key);
      if (Objects.nonNull(possibleJvmSpecifiedValue) && !possibleJvmSpecifiedValue.equals(configDefaultValue)) {
        configEntryValueField.put(PropertyNameMapper.mapToFrontendKeyName(key), possibleJvmSpecifiedValue);
      } else {
        configEntryValueField.put(PropertyNameMapper.mapToFrontendKeyName(key), configDefaultValue);
      }
    });
    jsonConfigEntry.put(VALUE, configEntryValueField.encode());
    return jsonConfigEntry;
  }

  /**
   * Updates system properties with values that has been returned from GET request to mod-configuration since they has the
   * highest priority. If GET returns nothing then default values (partially may be replaced with specified via JVM) are posted
   * {@link ModTenantAPI#postConfigIfAbsent(Context, HttpClientInterface, Map, Map.Entry)} and system properties already has been
   * updated with these posted values during {@link InitAPIs#init(Vertx, Context, Handler)}, so there no necessity of updating sys.
   * props. with them again and {@link ModTenantAPI#SHOULD_NOT_UPDATE_PROPS} tells about it.
   *
   * @param context - context
   * @param configEntry - configEntry returned from GET or POST request to mod-configuration
   * @return CompletableFuture.
   */
  private CompletableFuture<Map.Entry<String, JsonObject>> populateSystemProperties(Context context,
      Map.Entry<String, JsonObject> configEntry) {
    return VertxCompletableFuture.supplyBlockingAsync(context, () -> {
      JsonObject config = configEntry.getValue();
      if (config.containsKey(SHOULD_NOT_UPDATE_PROPS)) {
        return configEntry;
      }
      JsonObject configBody = config.getJsonArray(Constants.CONFIGS)
        .getJsonObject(CONFIG_JSON_BODY);
      Map<String, String> configKeyValueMap = configurationHelper.getConfigKeyValueMapFromJsonEntryValueField(configBody);
      Properties sysProps = System.getProperties();
      sysProps.putAll(configKeyValueMap);
      return configEntry;
    });
  }

  private CompletableFuture<Response> buildSuccessResponse(Context context) {
    return VertxCompletableFuture.supplyAsync(context, Response.noContent()
      .status(HttpStatus.SC_NO_CONTENT)::build);
  }

  private Future<Response> buildFailureResponse(String message) {
    Response response = Response.status(HttpStatus.SC_INTERNAL_SERVER_ERROR)
      .header(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.TEXT_PLAIN.toString())
      .entity(message)
      .build();
    return Future.succeededFuture(response);
  }

  private void handleException(CompletableFuture<Response> future, Throwable throwable) {
    logger.error(format("Cannot enable module: %s", throwable.getMessage()));
    future.completeExceptionally(throwable);
  }

  private Function<Throwable, Void> handleError(Handler<AsyncResult<Response>> asyncResultHandler) {
    return throwable -> {
      asyncResultHandler.handle(buildFailureResponse(throwable.getMessage()));
      return null;
    };
  }

}
