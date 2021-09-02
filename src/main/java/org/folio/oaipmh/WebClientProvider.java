package org.folio.oaipmh;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;

public class WebClientProvider {

  private static WebClient webClient;

  private WebClientProvider() {}

  public static void createWebClient(Vertx vertx) {
    webClient = WebClient.create(vertx);
  }

  public static WebClient getWebClient() {
    return webClient;
  }
}
