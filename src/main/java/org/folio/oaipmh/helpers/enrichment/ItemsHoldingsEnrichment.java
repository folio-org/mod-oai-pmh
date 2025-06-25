package org.folio.oaipmh.helpers.enrichment;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.processors.OaiPmhJsonParser;

import java.util.HashSet;
import java.util.Map;

import static org.folio.oaipmh.Constants.INSTANCE_ID_FIELD_NAME;
import static org.folio.oaipmh.Constants.LOCATION;
import static org.folio.oaipmh.Constants.SUPPRESS_FROM_DISCOVERY;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.CALL_NUMBER;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.HOLDINGS;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.ITEMS;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS;
import static org.folio.oaipmh.helpers.records.RecordMetadataManager.NAME;


public class ItemsHoldingsEnrichment {

  private static final String TEMPORARY_LOCATION = "temporaryLocation";
  private static final String PERMANENT_LOCATION = "permanentLocation";
  private static final String EFFECTIVE_LOCATION = "effectiveLocation";
  private static final String CODE = "code";
  private static final String HOLDINGS_RECORD_ID = "holdingsRecordId";
  private static final String ID = "id";
  private static final String COPY_NUMBER = "copyNumber";

  private final Map<String, JsonObject> instancesMap;
  private final Request request;
  private final boolean isSkipSuppressed;

  private static final Logger logger = LogManager.getLogger(ItemsHoldingsEnrichment.class);

  public ItemsHoldingsEnrichment(Map<String, JsonObject> instancesMap, Request request, boolean isSkipSuppressed) {
    this.instancesMap = instancesMap;
    this.request = request;
    this.isSkipSuppressed = isSkipSuppressed;
  }

  public JsonParser getJsonParser() {
    var jsonParser = new OaiPmhJsonParser()
      .objectValueMode();
    jsonParser.handler(event -> {
      JsonObject itemsAndHoldingsFields = event.objectValue();
      String instanceId = itemsAndHoldingsFields.getString(INSTANCE_ID_FIELD_NAME);
      JsonObject instance = instancesMap.get(instanceId);
      if (instance != null) {
        enrichDiscoverySuppressed(itemsAndHoldingsFields, instance);
        instance.put(RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS, itemsAndHoldingsFields);
        updateItems(instance);
        addEffectiveLocationCallNumberFromHoldingsWithoutItems(instance);
      } else {
        logger.info("enrichInstances:: For requestId {} instance with instanceId {} wasn't in the request", request.getRequestId(), instanceId);
      }
    });
    jsonParser.exceptionHandler(throwable ->
      logger.error("enrichInstances:: For requestId {} error has been occurred at JsonParser for items-and-holdings response, errors {}", request.getRequestId(), throwable.getMessage()));
    return jsonParser;
  }

  public Map<String, JsonObject> getInstancesMap() {
    return instancesMap;
  }

  public Request getRequest() {
    return request;
  }

  public boolean isSkipSuppressed() {
    return isSkipSuppressed;
  }

  private void enrichDiscoverySuppressed(JsonObject itemsAndHoldingsFields, JsonObject instance) {
    if (Boolean.parseBoolean(instance.getString(SUPPRESS_FROM_DISCOVERY)))
      for (Object item : itemsAndHoldingsFields.getJsonArray("items")) {
        if (item instanceof JsonObject) {
          JsonObject itemJson = (JsonObject) item;
          itemJson.put(RecordMetadataManager.INVENTORY_SUPPRESS_DISCOVERY_FIELD, true);
        }
      }
  }

  private void addEffectiveLocationCallNumberFromHoldingsWithoutItems(JsonObject instance) {
    JsonArray holdingsJson = instance.getJsonObject(ITEMS_AND_HOLDINGS_FIELDS)
      .getJsonArray(HOLDINGS);
    JsonArray itemsJson = instance.getJsonObject(ITEMS_AND_HOLDINGS_FIELDS)
      .getJsonArray(ITEMS);
    var excludeHoldingsIds = new HashSet<String>();
    for (Object item : itemsJson) {
      JsonObject itemJson = (JsonObject) item;
      excludeHoldingsIds.add(itemJson.getString(HOLDINGS_RECORD_ID));
    }
    for (Object holding : holdingsJson) {
      if (holding instanceof JsonObject) {
        JsonObject holdingJson = (JsonObject) holding;
        if (excludeHoldingsIds.contains(holdingJson.getString(ID))) continue;
        JsonObject callNumberJson = holdingJson.getJsonObject(CALL_NUMBER);
        JsonObject locationJson = holdingJson.getJsonObject(LOCATION);
        JsonObject effectiveLocationJson = locationJson.getJsonObject(EFFECTIVE_LOCATION);
        JsonObject itemJson = new JsonObject();
        itemJson.put(CALL_NUMBER, callNumberJson);
        JsonObject locationItemJson = new JsonObject();
        locationItemJson.put(NAME, effectiveLocationJson.getString(NAME));
        effectiveLocationJson.remove(NAME);
        locationItemJson.put(LOCATION, effectiveLocationJson);
        itemJson.put(LOCATION, locationItemJson);
        itemJson.put(RecordMetadataManager.INVENTORY_SUPPRESS_DISCOVERY_FIELD, Boolean.valueOf(holdingJson.getString(RecordMetadataManager.INVENTORY_SUPPRESS_DISCOVERY_FIELD)));
        itemJson.put(COPY_NUMBER, holdingJson.getString(COPY_NUMBER));
        itemsJson.add(itemJson);
      }
    }
  }

  private void updateItems(JsonObject instance) {
    JsonArray itemsJson = instance.getJsonObject(ITEMS_AND_HOLDINGS_FIELDS)
      .getJsonArray(ITEMS);
    for (Object item : itemsJson) {
      JsonObject itemJson = (JsonObject) item;
      itemJson.getJsonObject(LOCATION).put(NAME, itemJson.getJsonObject(LOCATION).getJsonObject(LOCATION).getString(NAME));
      itemJson.getJsonObject(LOCATION).getJsonObject(LOCATION).remove(NAME);
      itemJson.getJsonObject(LOCATION).getJsonObject(LOCATION).remove(CODE);
      itemJson.getJsonObject(LOCATION).remove(TEMPORARY_LOCATION);
      itemJson.getJsonObject(LOCATION).remove(PERMANENT_LOCATION);
    }
  }
}


