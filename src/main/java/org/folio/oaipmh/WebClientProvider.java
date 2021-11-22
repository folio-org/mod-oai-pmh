package org.folio.oaipmh;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.folio.oaipmh.Constants.REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC;

public class WebClientProvider {

  private static final Logger logger = LogManager.getLogger(WebClientProvider.class);

  private static final int DEFAULT_IDLE_TIMEOUT_SEC = 20;
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 2000;
  private static final int DOWNLOAD_INSTANCE_CONNECTION_TIMEOUT_MS = 604800000;
  private static final int KEEP_ALIVE_SEC = 1200;
  private static final int MAX_CHUNK_SIZE = 256;
  private static final int RECEIVE_BUFFER_SIZE = 10*1024;
  private static final String GET_IDLE_TIMEOUT_ERROR_MESSAGE = "Error occurred during resolving the idle timeout setting value. Setup client with default idle timeout " + DEFAULT_IDLE_TIMEOUT_SEC + " seconds.";

  private static Vertx vertx;
  private static WebClient webClient;
  private static WebClient webClientToDownloadInstances;
  private static final Map<String, WebClient> webClientForSRSPerTenant = new ConcurrentHashMap<>();

  private WebClientProvider() {}

  public static void init(Vertx v) {
    vertx = v;
    webClient = WebClient.create(vertx);
    WebClientOptions options = new WebClientOptions()
      .setKeepAliveTimeout(KEEP_ALIVE_SEC)
      .setConnectTimeout(DOWNLOAD_INSTANCE_CONNECTION_TIMEOUT_MS)
      .setMaxChunkSize(MAX_CHUNK_SIZE).setReceiveBufferSize(RECEIVE_BUFFER_SIZE);
    webClientToDownloadInstances = WebClient.create(vertx, options);
  }

  public static WebClient getWebClient() {
    return webClient;
  }

  public static WebClient getWebClientToDownloadInstances() {
    return webClientToDownloadInstances;
  }

  public static WebClient getWebClientForSRSByTenant(String tenant, String requestId) {
    return webClientForSRSPerTenant.computeIfAbsent(tenant, t -> WebClientProvider.createWebClientWithSRSConfiguredOptions(requestId));
  }

  public static void closeAll() {
    webClient.close();
    webClientToDownloadInstances.close();
    webClientForSRSPerTenant.values().forEach(WebClient::close);
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

