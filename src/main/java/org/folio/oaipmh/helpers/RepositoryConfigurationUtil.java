package org.folio.oaipmh.helpers;

import static org.folio.oaipmh.Constants.CONFIGS;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.OKAPI_URL;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.folio.rest.client.ConfigurationsClient;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;

public class RepositoryConfigurationUtil {

  private RepositoryConfigurationUtil() {

  }

  private static final Logger logger = LoggerFactory.getLogger(RepositoryConfigurationUtil.class);

  private static final String QUERY = "module==OAIPMH";

  private static ConfigurationHelper configurationHelper = ConfigurationHelper.getInstance();

  /**
   * Retrieve configuration for mod-oai-pmh from mod-configuration and puts these properties into context.
   * @param okapiHeaders
   * @param ctx the context
   * @return empty CompletableFuture
   */
  public static CompletableFuture<Void> loadConfiguration(Map<String, String> okapiHeaders, Context ctx) {

    String okapiURL = StringUtils.trimToEmpty(okapiHeaders.get(OKAPI_URL));
    String tenant = okapiHeaders.get(OKAPI_TENANT);
    String token = okapiHeaders.get(OKAPI_TOKEN);

    CompletableFuture<Void> future = new VertxCompletableFuture<>(ctx);
    try {
      ConfigurationsClient configurationsClient = new ConfigurationsClient(okapiURL, tenant, token, false);

      configurationsClient.getConfigurationsEntries(QUERY, 0, 100, null, null, response -> response.bodyHandler(body -> {

        try {
          if (response.statusCode() != 200) {
            logger.error("Error getting configuration for {} tenant. Expected status code 200 but was {}: {}",
              response.statusCode(), body);
            future.complete(null);
            return;
          }

          JsonObject config = new JsonObject();
          body.toJsonObject()
            .getJsonArray(CONFIGS)
            .stream()
            .map(Object::toString)
            .map(JsonObject::new)
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
          logger.error("Error getting configuration for {} tenant", e, tenant);
          future.complete(null);
        }
      }));
    } catch (Exception e) {
      logger.error("Error happened initializing mod-configurations client for {} tenant", e, tenant);
      future.complete(null);
    }
    return future;
  }

  /**
   * Gets value of the config either from shared config or from System properties as a fallback.
   * @param tenant tenant
   * @param name config key
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
}
