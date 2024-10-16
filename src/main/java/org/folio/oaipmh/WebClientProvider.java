package org.folio.oaipmh;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;

import static org.folio.oaipmh.Constants.REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC;

public class WebClientProvider {

  private static final Logger logger = LogManager.getLogger(WebClientProvider.class);

  private static final int DEFAULT_IDLE_TIMEOUT_SEC = 20;
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 2000;
  private static final String GET_IDLE_TIMEOUT_ERROR_MESSAGE = "Error occurred during resolving the idle timeout setting value. Setup client with default idle timeout " + DEFAULT_IDLE_TIMEOUT_SEC + " seconds.";

  private static Vertx vertx;
  @Getter
  private static WebClient webClient;

  private WebClientProvider() {}

  public static void init(Vertx v) {
    vertx = v;
    webClient = WebClient.create(vertx);
  }

  public static WebClient getWebClientForSrs(String requestId) {
    return WebClientProvider.createWebClientWithSRSConfiguredOptions(requestId);
  }

  public static void closeAll() {
    webClient.close();
  }

  private static WebClient createWebClientWithSRSConfiguredOptions(String requestId) {
    String val = RepositoryConfigurationUtil.getProperty(requestId, REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC);
    int idleTimeout = DEFAULT_IDLE_TIMEOUT_SEC;
    try {
      idleTimeout = Integer.parseInt(val);
      logger.debug("Setup client with idle timeout '{}' seconds", idleTimeout);
    } catch (Exception e) {
      logger.error(GET_IDLE_TIMEOUT_ERROR_MESSAGE, e);
    }
    WebClientOptions webClientOptions = new WebClientOptions()
      .setKeepAlive(true)
      .setIdleTimeout(idleTimeout)
      .setConnectTimeout(DEFAULT_CONNECTION_TIMEOUT_MS);
     return  WebClient.create(vertx, webClientOptions);
  }
}

