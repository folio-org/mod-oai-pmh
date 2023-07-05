package org.folio.oaipmh.helpers.client;

import io.vertx.core.json.JsonObject;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class ConsortiaClient extends InventoryClient {

  private static final Logger logger = LogManager.getLogger(ConsortiaClient.class);

  private static final String CENTRAL_TENANT_ID_ENDPOINT = "/user-tenants";

  public List<String> getUserTenants(Request request) {
    List<String> result = new ArrayList<>();
    var endpoint = request.getOkapiUrl() + CENTRAL_TENANT_ID_ENDPOINT;
    HttpGet httpGet = httpGet(request, endpoint);
    logger.info("Calling GET {}", endpoint);
    try (CloseableHttpResponse response = HttpClients.createDefault().execute(httpGet)) {
      var jsonObject = getResponseEntity(response);
      var jsonArray = jsonObject.getJsonArray("userTenants");
      jsonArray.stream().map(item -> (JsonObject)item)
        .forEach(itemJson -> result.add(itemJson.getString("centralTenantId")));
    } catch (Exception exception) {
      logger.error("Exception while calling {}", httpGet.getURI(), exception);
    }
    return result;
  }
}
