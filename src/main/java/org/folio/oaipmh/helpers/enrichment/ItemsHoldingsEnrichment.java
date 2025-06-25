package org.folio.oaipmh.helpers.enrichment;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.processors.OaiPmhJsonParser;
import org.folio.oaipmh.helpers.referencedata.ReferenceDataProvider;
import org.folio.oaipmh.helpers.referencedata.ReferenceData;

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
  private final ReferenceDataProvider referenceDataProvider;

  private static final Logger logger = LogManager.getLogger(ItemsHoldingsEnrichment.class);

  public ItemsHoldingsEnrichment(Map<String, JsonObject> instancesMap, Request request, boolean isSkipSuppressed) {
    this.instancesMap = instancesMap;
    this.request = request;
    this.isSkipSuppressed = isSkipSuppressed;
    this.referenceDataProvider = null;
  }

  public ItemsHoldingsEnrichment(Map<String, JsonObject> instancesMap, Request request, boolean isSkipSuppressed, ReferenceDataProvider referenceDataProvider) {
    this.instancesMap = instancesMap;
    this.request = request;
    this.isSkipSuppressed = isSkipSuppressed;
    this.referenceDataProvider = referenceDataProvider;
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
        // Enrich location reference data for all items
        enrichLocationReferenceData(itemsAndHoldingsFields);
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

  /**
   * Enriches location data with reference information (institution, campus, library names)
   * This is particularly important for inactive locations which may not have this data populated
   */
  private void enrichLocationReferenceData(JsonObject itemsAndHoldingsFields) {
    if (referenceDataProvider == null) {
      logger.debug("Reference data provider not available, skipping location enrichment");
      return;
    }
    
    try {
      ReferenceData referenceData = referenceDataProvider.get(request);
      Map<String, JsonObject> locations = referenceData.get("locations");
      Map<String, JsonObject> institutions = referenceData.get("institutions");
      Map<String, JsonObject> campuses = referenceData.get("campuses");
      Map<String, JsonObject> libraries = referenceData.get("libraries");
      
      // Enrich items
      JsonArray items = itemsAndHoldingsFields.getJsonArray(ITEMS);
      if (items != null) {
        items.forEach(item -> {
          JsonObject itemJson = (JsonObject) item;
          enrichItemLocationData(itemJson, locations, institutions, campuses, libraries);
        });
      }
      
    } catch (Exception e) {
      logger.error("Error enriching location reference data for request {}: {}", request.getRequestId(), e.getMessage(), e);
    }
  }

  /**
   * Enriches a single item's location data with reference information
   */
  private void enrichItemLocationData(JsonObject item, Map<String, JsonObject> locationMap, 
                                     Map<String, JsonObject> institutionMap, Map<String, JsonObject> campusMap, 
                                     Map<String, JsonObject> libraryMap) {
    JsonObject locationWrapper = item.getJsonObject(LOCATION);
    if (locationWrapper != null) {
      JsonObject effectiveLocation = locationWrapper.getJsonObject(LOCATION);
      if (effectiveLocation != null && effectiveLocation.getString("id") != null) {
        String locationId = effectiveLocation.getString("id");
        JsonObject fullLocationData = locationMap.get(locationId);
        
        if (fullLocationData != null) {
          logger.info("Enriching location {} (isActive: {}) with reference data", 
                     locationId, fullLocationData.getBoolean("isActive"));
          
          // Add institution name
          String institutionId = fullLocationData.getString("institutionId");
          if (institutionId != null && institutionMap.containsKey(institutionId)) {
            String institutionName = institutionMap.get(institutionId).getString("name");
            effectiveLocation.put("institutionName", institutionName);
            logger.info("Added institutionName: {}", institutionName);
          }
          
          // Add campus name
          String campusId = fullLocationData.getString("campusId");
          if (campusId != null && campusMap.containsKey(campusId)) {
            String campusName = campusMap.get(campusId).getString("name");
            effectiveLocation.put("campusName", campusName);
            logger.info("Added campusName: {}", campusName);
          }
          
          // Add library name
          String libraryId = fullLocationData.getString("libraryId");
          if (libraryId != null && libraryMap.containsKey(libraryId)) {
            String libraryName = libraryMap.get(libraryId).getString("name");
            effectiveLocation.put("libraryName", libraryName);
            logger.info("Added libraryName: {}", libraryName);
          }
          
          // Make sure isActive status is preserved
          effectiveLocation.put("isActive", fullLocationData.getBoolean("isActive", true));
        }
      }
    }
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


