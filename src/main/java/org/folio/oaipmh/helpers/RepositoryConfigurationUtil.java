package org.folio.oaipmh.helpers;

import static java.lang.Boolean.parseBoolean;
import static org.folio.oaipmh.Constants.CONFIGS;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.OKAPI_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;
import static org.openarchives.oai._2.DeletedRecordType.PERSISTENT;
import static org.openarchives.oai._2.DeletedRecordType.TRANSIENT;

import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.client.ConfigurationsClient;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.DeletedRecordType;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class RepositoryConfigurationUtil {

  private RepositoryConfigurationUtil() {

  }

  private static final Logger logger = LogManager.getLogger(RepositoryConfigurationUtil.class);

  private static final String QUERY = "module==OAIPMH";

  private static ConfigurationHelper configurationHelper = ConfigurationHelper.getInstance();

  /**
   * Retrieve configuration for mod-oai-pmh from mod-configuration and puts these properties into context.
   *
   * @param okapiHeaders
   * @param ctx          the context
   * @return empty CompletableFuture
   */
  public static CompletableFuture<Void> loadConfiguration(Map<String, String> okapiHeaders, Context ctx) {
    CompletableFuture<Void> future = new VertxCompletableFuture<>(ctx);

    String okapiURL = StringUtils.trimToEmpty(okapiHeaders.get(OKAPI_URL));
    String tenant = okapiHeaders.get(OKAPI_TENANT);
    String token = okapiHeaders.get(OKAPI_TOKEN);

    try {
      ConfigurationsClient configurationsClient = new ConfigurationsClient(okapiURL, tenant, token, false);
      Promise<HttpResponse<Buffer>> responsePromise = Promise.promise();
      configurationsClient.getConfigurationsEntries(QUERY, 0, 100, null, null, responsePromise);
      responsePromise.future().onSuccess(response -> {
        try {
          if (response.statusCode() != 200) {
            logger.error("Error getting configuration for {} tenant. Expected status code 200 but was {}: {}", tenant,
              response.statusCode(), response.body());
            future.complete(null);
            return;
          }

          JsonObject config = new JsonObject();
          response.bodyAsJsonObject()
            .getJsonArray(CONFIGS)
            .stream()
            .map(object -> (JsonObject) object)
            .map(configurationHelper::getConfigKeyValueMapFromJsonEntryValueField)
            .forEach(configKeyValueMap -> configKeyValueMap.forEach(config::put));

          JsonObject tenantConfig = ctx.config().getJsonObject(tenant);
          if (tenantConfig != null) {
            tenantConfig.mergeIn(config);
          } else {
            ctx.config().put(tenant, config);
          }

          future.complete(null);
        } catch (Exception e) {
          logger.error("Error getting configuration for {} tenant", tenant);
          future.completeExceptionally(e);
        }
      }).onFailure(e -> {
        logger.error("Error happened initializing mod-configurations client for {} tenant", tenant);
        future.completeExceptionally(e);
      });
    } catch (Exception e) {
      logger.error("Error happened initializing mod-configurations client for {} tenant", tenant);
      future.completeExceptionally(e);
    }
    return future;
  }

  /**
   * Gets value of the config either from shared config or from System properties as a fallback.
   *
   * @param tenant tenant
   * @param name   config key
   * @return value of the config either from shared config if present. Or from System properties as fallback.
   */
  public static String getProperty(String tenant, String name) {
    JsonObject configs = Vertx.currentContext().config().getJsonObject(tenant);
    String defaultValue = System.getProperty(name);
    if (configs != null) {
      return configs.getString(name, defaultValue);
    }
    return defaultValue;
  }

  public static boolean getBooleanProperty(Map<String, String> okapiHeaders, String name) {
    String tenant = TenantTool.tenantId(okapiHeaders);
    JsonObject configs = Vertx.currentContext().config().getJsonObject(tenant);
    String defaultValue = System.getProperty(name);
    if (configs != null) {
      return parseBoolean(configs.getString(name, defaultValue));
    }
    return parseBoolean(defaultValue);
  }

  public static boolean isDeletedRecordsEnabled(Request request) {
    String tenant = TenantTool.tenantId(request.getOkapiHeaders());
    String propertyName = getProperty(tenant, REPOSITORY_DELETED_RECORDS);
    try {
      DeletedRecordType deletedRecordType = DeletedRecordType.fromValue(propertyName);
      return Arrays.asList(PERSISTENT, TRANSIENT).contains(deletedRecordType);
    } catch (IllegalArgumentException ex) {
      String defaultPropertyValue = System.getProperty(REPOSITORY_DELETED_RECORDS);
      return Boolean.parseBoolean(defaultPropertyValue);
    }
  }
}
