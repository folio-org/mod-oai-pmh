package org.folio.oaipmh.helpers;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.WebClientProvider;
import org.folio.rest.tools.utils.TenantTool;

import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.HTTPS;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;

public class ItemsHoldingInventoryRequestFactory {
  public static final String INVENTORY_ITEMS_AND_HOLDINGS_ENDPOINT = "/inventory-hierarchy/items-and-holdings";

  public static HttpRequest<Buffer> getItemsHoldingsInventoryRequest(Request request) {
    var webClient = WebClientProvider.getWebClient();
    var httpRequest = webClient.postAbs(request.getOkapiUrl() + INVENTORY_ITEMS_AND_HOLDINGS_ENDPOINT);
    if (request.getOkapiUrl()
      .contains(HTTPS)) {
      httpRequest.ssl(true);
    }
    httpRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpRequest.putHeader(ACCEPT, APPLICATION_JSON);
    httpRequest.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    return httpRequest;
  }
}
