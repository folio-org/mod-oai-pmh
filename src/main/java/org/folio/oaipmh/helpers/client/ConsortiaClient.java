package org.folio.oaipmh.helpers.client;

import io.vertx.core.json.JsonArray;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.springframework.stereotype.Component;

@Component
public class ConsortiaClient extends InventoryClient {

  private static final Logger logger = LogManager.getLogger(ConsortiaClient.class);

  private static final String USER_TENANTS_ENDPOINT = "/user-tenants?limit=1";

  public JsonArray getUserTenants(Request request) {
    var endpoint = request.getOkapiUrl() + USER_TENANTS_ENDPOINT;
    HttpGet httpGet = httpGet(request, endpoint);
    logger.info("Calling GET {}", endpoint);
    try (CloseableHttpResponse response = HttpClients.createDefault().execute(httpGet)) {
      var jsonObject = getResponseEntity(response);
      return jsonObject.getJsonArray("userTenants");
    } catch (Exception exception) {
      logger.error("Exception while calling {}", httpGet.getURI(), exception);
    }
    return new JsonArray();
  }
}
