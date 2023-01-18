package org.folio.oaipmh.helpers.client;

import io.vertx.core.json.JsonObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.rest.tools.utils.TenantTool;
import org.springframework.stereotype.Component;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.folio.oaipmh.Constants.ALTERNATIVE_TITLE_TYPES_URI;
import static org.folio.oaipmh.Constants.CALL_NUMBER_TYPES_URI;
import static org.folio.oaipmh.Constants.CAMPUSES_URI;
import static org.folio.oaipmh.Constants.CONTRIBUTOR_NAME_TYPES_URI;
import static org.folio.oaipmh.Constants.ELECTRONIC_ACCESS_RELATIONSHIPS_URI;
import static org.folio.oaipmh.Constants.HOLDINGS_NOTE_TYPES_URI;
import static org.folio.oaipmh.Constants.IDENTIFIER_TYPES_URI;
import static org.folio.oaipmh.Constants.INSTANCE_FORMATS_URI;
import static org.folio.oaipmh.Constants.INSTITUTIONS_URI;
import static org.folio.oaipmh.Constants.ITEM_NOTE_TYPES_URI;
import static org.folio.oaipmh.Constants.LIBRARIES_URI;
import static org.folio.oaipmh.Constants.LOANTYPES_URI;
import static org.folio.oaipmh.Constants.LOCATION_URI;
import static org.folio.oaipmh.Constants.MATERIAL_TYPES_URI;
import static org.folio.oaipmh.Constants.MODES_OF_ISSUANCE_URI;
import static org.folio.oaipmh.Constants.NATURE_OF_CONTENT_TERMS_URI;
import static org.folio.oaipmh.Constants.RESOURCE_TYPES_URI;
import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;
import static org.folio.rest.RestVerticle.OKAPI_HEADER_TOKEN;

@Component
public class InventoryClient {

  private static final Logger logger = LogManager.getLogger(InventoryClient.class);
  private static final int REFERENCE_DATA_LIMIT = 1000;
  private static final String ENDPOINT_PATTERN = "%s%s?limit=%d";
  public Map<String, JsonObject> getAlternativeTitleTypes(Request request) {
    return get(request, ALTERNATIVE_TITLE_TYPES_URI, "alternativeTitleTypes");
  }

  public Map<String, JsonObject> getContributorNameTypes(Request request) {
    return get(request, CONTRIBUTOR_NAME_TYPES_URI, "contributorNameTypes");
  }

  public Map<String, JsonObject> getElectronicAccessRelationships(Request request) {
    return get(request, ELECTRONIC_ACCESS_RELATIONSHIPS_URI, "electronicAccessRelationships");
  }

  public Map<String, JsonObject> getInstanceTypes(Request request) {
    return get(request, RESOURCE_TYPES_URI, "instanceTypes");
  }

  public Map<String, JsonObject> getIdentifierTypes(Request request) {
    return get(request, IDENTIFIER_TYPES_URI, "identifierTypes");
  }

  public Map<String, JsonObject> getModesOfIssuance(Request request) {
    return get(request, MODES_OF_ISSUANCE_URI, "issuanceModes");
  }

  public Map<String, JsonObject> getHoldingsNoteTypes(Request request) {
    return get(request, HOLDINGS_NOTE_TYPES_URI, "holdingsNoteTypes");
  }

  public Map<String, JsonObject> getItemNoteTypes(Request request) {
    return get(request, ITEM_NOTE_TYPES_URI, "itemNoteTypes");
  }

  public Map<String, JsonObject> getNatureOfContentTerms(Request request) {
    return get(request, NATURE_OF_CONTENT_TERMS_URI, "natureOfContentTerms");
  }

  public Map<String, JsonObject> getLocations(Request request) {
    return get(request, LOCATION_URI, "locations");
  }

  public Map<String, JsonObject> getLoanTypes(Request request) {
    return get(request, LOANTYPES_URI, "loantypes");
  }

  public Map<String, JsonObject> getLibraries(Request request) {
    return get(request, LIBRARIES_URI, "loclibs");
  }

  public Map<String, JsonObject> getCampuses(Request request) {
    return get(request, CAMPUSES_URI, "loccamps");
  }

  public Map<String, JsonObject> getInstitutions(Request request) {
    return get(request, INSTITUTIONS_URI, "locinsts");
  }

  public Map<String, JsonObject> getMaterialTypes(Request request) {
    return get(request, MATERIAL_TYPES_URI, "mtypes");
  }

  public Map<String, JsonObject> getInstanceFormats(Request request) {
    return get(request, INSTANCE_FORMATS_URI, "instanceFormats");
  }

  public Map<String, JsonObject> getCallNumberTypes(Request request) {
    return get(request, CALL_NUMBER_TYPES_URI, "callNumberTypes");
  }

  private Map<String, JsonObject> get(Request request, String endpoint, String key) {
    Map<String, JsonObject> map = new HashMap<>();
    endpoint = format(ENDPOINT_PATTERN, request.getOkapiUrl(), endpoint, REFERENCE_DATA_LIMIT);
    HttpGet httpGet = new HttpGet();
    httpGet.setHeader(OKAPI_HEADER_TOKEN, request.getOkapiToken());
    httpGet.setHeader(OKAPI_HEADER_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpGet.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    httpGet.setHeader((HttpHeaders.ACCEPT), MediaType.APPLICATION_JSON);
    httpGet.setURI(URI.create(endpoint));
    logger.info("Calling GET {}", endpoint);
    try (CloseableHttpResponse response = HttpClients.createDefault().execute(httpGet)) {
      var jsonObject = getResponseEntity(response);
      var jsonArray = jsonObject.getJsonArray(key);
      jsonArray.forEach(json -> map.put(((JsonObject) json).getString("id"), (JsonObject) json));
    } catch (Exception exception) {
      logger.error("Exception while calling {}", httpGet.getURI(), exception);
    }
    return map;
  }

  private static JsonObject getResponseEntity(CloseableHttpResponse response) throws IOException {
    HttpEntity entity = response.getEntity();
    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK && entity != null) {
      try {
        var body = EntityUtils.toString(entity);
        logger.debug("Response body: {}", body);
        return new JsonObject(body);
      } catch (IOException e) {
        logger.error("Exception while building response entity", e);
      }
    }
    throw new IOException("Get invalid response with status: " + response.getStatusLine().getStatusCode());
  }
}
