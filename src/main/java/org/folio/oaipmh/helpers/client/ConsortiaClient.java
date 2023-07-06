package org.folio.oaipmh.helpers.client;

import io.vertx.core.json.JsonArray;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.springframework.stereotype.Component;

import static java.lang.String.format;

@Component
public class ConsortiaClient extends InventoryClient {

  private static final Logger logger = LogManager.getLogger(ConsortiaClient.class);

  private static final String USER_TENANTS_ENDPOINT = "/user-tenants?limit=1";
  private static final String CONSORTIUM_ENDPOINT = "/consortia";
  private static final String TENANTS_ENDPOINT = "/consortia/%s/tenants";

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

  public JsonArray getConsortiums(Request request) {
    var endpoint = request.getOkapiUrl() + CONSORTIUM_ENDPOINT;
    HttpGet httpGet = httpGet(request, endpoint);
    logger.info("Calling GET {}", endpoint);
    try (CloseableHttpResponse response = HttpClients.createDefault().execute(httpGet)) {
      var jsonObject = getResponseEntity(response);
      return jsonObject.getJsonArray("consortia");
    } catch (Exception exception) {
      logger.error("Exception while calling {}", httpGet.getURI(), exception);
    }
    return new JsonArray();
  }

  public JsonArray getTenants(Request request, String consortiumId) {
    var endpoint = request.getOkapiUrl() + format(TENANTS_ENDPOINT, consortiumId);
    HttpGet httpGet = httpGet(request, endpoint);
    logger.info("Calling GET {}", endpoint);
    try (CloseableHttpResponse response = HttpClients.createDefault().execute(httpGet)) {
      var jsonObject = getResponseEntity(response);
      return jsonObject.getJsonArray("tenants");
    } catch (Exception exception) {
      logger.error("Exception while calling {}", httpGet.getURI(), exception);
    }
    return new JsonArray();
  }
}
