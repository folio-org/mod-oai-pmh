package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.client.HttpClientFactory;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;

public class TenantAPIs extends TenantAPI {
  private final Logger logger = LoggerFactory.getLogger(TenantAPIs.class);

  private static final String X_OKAPI_URL = "x-okapi-url";
  private static final String X_OKAPI_TENANT = "x-okapi-tenant";
  private static final String JSON_BEHAVIOUR_CONFIG_PATH = "config" + File.separator + "behaviorConfigs.json";
  private static final String JSON_GENERAL_CONFIG_PATH = "config" + File.separator + "generalConfigs.json";
  private static final String JSON_TECHNICAL_CONFIG_PATH = "config" + File.separator + "technicalConfigs.json";
  private static final String MOD_CONFIGURATION_GET = "/configurations/entries";

  @Override
  public void postTenant(final TenantAttributes entity, final Map<String, String> headers,
      final Handler<AsyncResult<Response>> handlers, final Context context) {
    logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@ POST @@@@@@@@@@@@@@@@@@@@@@@@@");
    logTenantData(headers);
    super.postTenant(entity, headers, handlers, context);
  }

  @Override
  public void deleteTenant(final Map<String, String> headers, final Handler<AsyncResult<Response>> handlers,
      final Context context) {
    logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@ DELETE @@@@@@@@@@@@@@@@@@@@@@@@@");
    logTenantData(headers);
    super.deleteTenant(headers, handlers, context);
  }

  private void logTenantData(final Map<String, String> headers) {
    StringBuilder stringBuilder = new StringBuilder("====================TENANT API====================")
      .append(System.lineSeparator());

    headers.forEach((key, value) -> addHeaderKeyValue(stringBuilder, key, value));
    logger.info(stringBuilder.toString());
  }

  private void addHeaderKeyValue(StringBuilder stringBuilder, String key, String value) {
    stringBuilder.append("KEY : ")
      .append(key)
      .append(" ")
      .append("VALUE : ")
      .append(value)
      .append(System.lineSeparator());
  }

  private void loadConfigData(Map<String,String> headers) {
    String okapiUrl = headers.get(X_OKAPI_URL);
    String tenant = headers.get(X_OKAPI_TENANT);

    Map<String,String> okapiHeaders = new HashMap<>();
    JsonObject behaviourConf = new JsonObject(getResourceAsString(JSON_BEHAVIOUR_CONFIG_PATH));
    JsonObject generalConf = new JsonObject(getResourceAsString(JSON_GENERAL_CONFIG_PATH));
    JsonObject technicalConf = new JsonObject(getResourceAsString(JSON_TECHNICAL_CONFIG_PATH));

    HttpClientInterface httpClient = HttpClientFactory.getHttpClient(okapiUrl, tenant, true);
    try{
      httpClient.request(MOD_CONFIGURATION_GET, okapiHeaders, true)
        .thenApply(this::populateSystemPropertiesIfConfigAlreadyExists)
        .thenAccept(value -> postConfigIfAbsent(value.get(),httpClient))
        .thenAccept(value-> httpClient.closeClient());
    }catch (Exception ex){

    }
  }

  private CompletableFuture<Boolean> populateSystemPropertiesIfConfigAlreadyExists(org.folio.rest.tools.client.Response response){

  }

  private CompletableFuture<Boolean> postConfigIfAbsent(boolean isAbsent, HttpClientInterface httpClient){

  }

  private String getResourceAsString(String resourcePath){
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
      if(is ==null) throw new IllegalArgumentException(String.format("Unable open the resource file %s",resourcePath));
      try (InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
           BufferedReader reader = new BufferedReader(isr)) {
        return reader.lines().collect(Collectors.joining(System.lineSeparator()));
      }
    }catch (IOException ex){
      ex.printStackTrace();
      return "error";
    }
  }

}
