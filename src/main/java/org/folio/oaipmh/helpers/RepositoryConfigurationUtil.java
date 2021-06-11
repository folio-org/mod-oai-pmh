package org.folio.oaipmh.helpers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.folio.rest.client.ConfigurationsClient;
import org.folio.rest.tools.utils.VertxUtils;
import org.openarchives.oai._2.DeletedRecordType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Boolean.parseBoolean;
import static org.folio.oaipmh.Constants.*;
import static org.openarchives.oai._2.DeletedRecordType.PERSISTENT;
import static org.openarchives.oai._2.DeletedRecordType.TRANSIENT;

public class RepositoryConfigurationUtil {

  private static final Logger logger = LogManager.getLogger(RepositoryConfigurationUtil.class);

  private static final String QUERY = "module==OAIPMH";
  private static Map<String, JsonObject> configsMap = new HashMap<>();
  private static ConfigurationHelper configurationHelper = ConfigurationHelper.getInstance();

  private RepositoryConfigurationUtil() {}

  /**
   * Retrieve configuration for mod-oai-pmh from mod-configuration and puts these properties into context.
   *
   * @param okapiHeaders - okapi headers
   * @param requestId - unique identifier for current request
   * @return empty CompletableFuture
   */
  public static Future<Void> loadConfiguration(Map<String, String> okapiHeaders, String requestId) {
    Promise<Void> promise = Promise.promise();

    String okapiURL = StringUtils.trimToEmpty(okapiHeaders.get(OKAPI_URL));
    String tenant = okapiHeaders.get(OKAPI_TENANT);
    String token = okapiHeaders.get(OKAPI_TOKEN);

    WebClientOptions options = new WebClientOptions().setKeepAlive(false);
    var client = WebClient.create(VertxUtils.getVertxFromContextOrNew(), options);

    try {
      var configurationsClient = new ConfigurationsClient(okapiURL, tenant, token, client);
      configurationsClient.getConfigurationsEntries(QUERY, 0, 100, null, null, result -> {
        try {
          if (result.succeeded()) {
            HttpResponse<Buffer> response = result.result();
            JsonObject body = response.bodyAsJsonObject();
            if (response.statusCode() != 200) {
              logger.error("Error getting configuration for {} tenant. Expected status code 200 but was {}: {}.", tenant, response.statusCode(), body);
              promise.complete(null);
              return;
            }

            JsonObject config = new JsonObject();
            body.getJsonArray(CONFIGS)
              .stream()
              .map(object -> (JsonObject) object)
              .map(configurationHelper::getConfigKeyValueMapFromJsonEntryValueField)
              .forEach(configKeyValueMap -> configKeyValueMap.forEach(config::put));

            configsMap.put(requestId, config);
            promise.complete(null);
          }
        } catch (Exception e) {
          logger.error("Error occurred while processing configuration for {} tenant.", tenant, e);
          promise.fail(e);
        }
      });
    } catch (Exception e) {
      logger.error("Error happened initializing mod-configurations client for {} tenant.", tenant, e);
      promise.fail(e);
      return promise.future().onComplete(notUsed -> client.close());
    }
    return promise.future();
  }

  public static void replaceGeneratedConfigKeyWithExisted(String generatedRequestId, String existedRequestId) {
    configsMap.put(existedRequestId, configsMap.remove(generatedRequestId));
  }

  /**
   * Gets value of the config either from shared config or from System properties as a fallback.
   *
   * @param requestId requestId
   * @param name      config key
   * @return value of the config either from shared config if present. Or from System properties as fallback.
   */
  public static String getProperty(String requestId, String name) {
    JsonObject configs = getConfig(requestId);
    String defaultValue = System.getProperty(name);
    if (configs != null) {
      return configs.getString(name, defaultValue);
    }
    return defaultValue;
  }

  public static boolean getBooleanProperty(String requestId, String name) {
    JsonObject configs = getConfig(requestId);
    String defaultValue = System.getProperty(name);
    if (configs != null) {
      return parseBoolean(configs.getString(name, defaultValue));
    }
    return parseBoolean(defaultValue);
  }

  public static boolean isDeletedRecordsEnabled(String requestId) {
    String propertyName = getProperty(requestId, REPOSITORY_DELETED_RECORDS);
    try {
      DeletedRecordType deletedRecordType = DeletedRecordType.fromValue(propertyName);
      return Arrays.asList(PERSISTENT, TRANSIENT).contains(deletedRecordType);
    } catch (IllegalArgumentException ex) {
      String defaultPropertyValue = System.getProperty(REPOSITORY_DELETED_RECORDS);
      return Boolean.parseBoolean(defaultPropertyValue);
    }
  }

  public static void cleanConfigForRequestId(String requestId) {
    configsMap.remove(requestId);
  }

  public static JsonObject getConfig(String requestId) {
    return configsMap.get(requestId);
  }
}
