package org.folio.rest.impl;

import static java.lang.String.format;
import static me.escoffier.vertx.completablefuture.VertxCompletableFuture.supplyAsync;
import static org.folio.oaipmh.Constants.CONFIGS_SET;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import org.apache.http.HttpStatus;
import org.folio.oaipmh.helpers.resource.ResourceHelper;
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
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;

public class ModTenantAPI extends TenantAPI {
  private final Logger logger = LoggerFactory.getLogger(ModTenantAPI.class);

  private static final String X_OKAPI_URL = "x-okapi-url";
  private static final String X_OKAPI_TENANT = "x-okapi-tenant";
  private static final String MOD_CONFIGURATION_ENTRIES_URI = "/configurations/entries";
  private static final String CONFIG_NAME = "configName";
  private static final String ENABLED = "enabled";
  private static final String CONFIGS = "configs";
  private static final String CONFIG_DIR_NAME = "config";
  private static final String VALUE = "value";
  private static final int CONFIG_JSON_BODY = 0;
  private ResourceHelper resourceHelper = new ResourceHelper();

  @Override
  public void postTenant(final TenantAttributes entity, final Map<String, String> headers,
      final Handler<AsyncResult<Response>> handlers, final Context context) {
    loadConfigData(headers, context).thenAccept(response -> handlers.handle(Future.succeededFuture(response)))
      .exceptionally(handleError(handlers));
  }

  private CompletableFuture<Response> loadConfigData(Map<String, String> headers, Context context) {
    VertxCompletableFuture<Response> future = new VertxCompletableFuture<>(context);

    String okapiUrl = headers.get(X_OKAPI_URL);
    String tenant = headers.get(X_OKAPI_TENANT);

    HttpClientInterface httpClient = HttpClientFactory.getHttpClient(okapiUrl, tenant, true);
    List<CompletableFuture<Void>> completableFutures = new ArrayList<>();

    CONFIGS_SET.forEach(config -> completableFutures.add(requestConfig(context, httpClient, headers, config)
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
    return MOD_CONFIGURATION_ENTRIES_URI.concat(queryBuilder.build());
  }

  private CompletableFuture<Map.Entry<String, JsonObject>> postConfigIfAbsent(Context context, HttpClientInterface httpClient,
      Map<String, String> headers, Map.Entry<String, JsonObject> configPair) {
    JsonObject config = configPair.getValue();
    JsonArray configs = config.getJsonArray(CONFIGS);
    if (configs.isEmpty()) {
      JsonObject configToPost = resourceHelper.getJsonConfigFromResources(CONFIG_DIR_NAME, configPair.getKey());
      try {
        headers.put(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.APPLICATION_JSON.toString());
        return httpClient.request(HttpMethod.POST, configToPost, MOD_CONFIGURATION_ENTRIES_URI, headers)
          .thenCompose(response -> supplyAsync(context, () -> new HashMap.SimpleImmutableEntry<>(configPair.getKey(), response.getBody())));
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
      //set boolean flag on prev step to indicate whether it is needed to populate prop-s
      JsonObject config = configPair.getValue()
        .getJsonArray(CONFIGS)
        .getJsonObject(CONFIG_JSON_BODY);
      Map<String, String> configKeyValueMap = resourceHelper.getConfigKeyValueMapFromJsonConfigEntry(config);
      Properties sysProps = System.getProperties();
      sysProps.putAll(configKeyValueMap);
      return configPair;
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
