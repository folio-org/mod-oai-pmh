package org.folio.rest.impl;

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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import org.folio.oaipmh.helpers.storage.CQLQueryBuilder;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.client.HttpClientFactory;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import javafx.util.Pair;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;

public class TenantAPIs extends TenantAPI {
  private final Logger logger = LoggerFactory.getLogger(TenantAPIs.class);

  private static final String X_OKAPI_URL = "x-okapi-url";
  private static final String X_OKAPI_TENANT = "x-okapi-tenant";
  private static final String JSON_BEHAVIOUR_CONFIG_PATH = "config" + File.separator + "behaviorConfigs.json";
  private static final String JSON_GENERAL_CONFIG_PATH = "config" + File.separator + "generalConfigs.json";
  private static final String JSON_TECHNICAL_CONFIG_PATH = "config" + File.separator + "technicalConfigs.json";
  private static final String MOD_CONFIGURATION_ENTRIES_PATH = "/configurations/entries/";
  private static final String BEHAVIOUR = "behaviour";
  private static final String GENERAL = "general";
  private static final String TECHNICAL = "technical";
  private static final String CONFIG_NAME = "configName";
  private static final String ACTIVE = "active";

  @Override
  public void postTenant(final TenantAttributes entity, final Map<String, String> headers,
      final Handler<AsyncResult<Response>> handlers, final Context context) {
    Set<String> configSet = new HashSet<>(Arrays.asList(BEHAVIOUR, GENERAL, TECHNICAL));
    loadConfigData();
    super.postTenant(entity, headers, handlers, context);
  }

  private CompletableFuture<Response> loadConfigData(Map<String,String> headers, Set<String> configs, Context context) {
    CompletableFuture<Response> future = new VertxCompletableFuture<>(context);
    String okapiUrl = headers.get(X_OKAPI_URL);
    String tenant = headers.get(X_OKAPI_TENANT);

    HttpClientInterface httpClient = HttpClientFactory.getHttpClient(okapiUrl, tenant, true);
    List<CompletableFuture<Void>> completableFutures = new ArrayList<>();

    configs.forEach(config -> {
      completableFutures.add(requestConfig(httpClient, config, context).thenCompose(loadData))
    });
    }

  private CompletableFuture<Pair<String, JsonObject>> requestConfig(HttpClientInterface httpClient, String configName, Context context) {
    try {
      return httpClient.request(getConfigUrl(configName))
        .thenApply(response -> new Pair<>(configName, response.getBody()));
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  private String getConfigUrl(String configName) throws UnsupportedEncodingException {
    CQLQueryBuilder queryBuilder = new CQLQueryBuilder();
    queryBuilder.addStrictCriteria(CONFIG_NAME, configName)
      .and();
    queryBuilder.addStrictCriteria(ACTIVE, Boolean.TRUE.toString());
    return MOD_CONFIGURATION_ENTRIES_PATH.concat(queryBuilder.build());
  }

  private CompletableFuture<Boolean> populateSystemPropertiesIfConfigAlreadyExists(org.folio.rest.tools.client.Response response) {

  }

  private CompletableFuture<Boolean> postConfigIfAbsent(boolean isAbsent, HttpClientInterface httpClient) {

  }

  private String getResourceAsString(String resourcePath) {
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
      if (is == null)
        throw new IllegalArgumentException(String.format("Unable open the resource file %s", resourcePath));
      try (InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
          BufferedReader reader = new BufferedReader(isr)) {
        return reader.lines()
          .collect(Collectors.joining(System.lineSeparator()));
      }
    } catch (IOException ex) {
      ex.printStackTrace();
      return "error";
    }
  }

}
