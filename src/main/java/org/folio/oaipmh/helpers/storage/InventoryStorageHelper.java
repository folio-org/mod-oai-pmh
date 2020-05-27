package org.folio.oaipmh.helpers.storage;

import java.io.UnsupportedEncodingException;

import org.folio.oaipmh.Request;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class InventoryStorageHelper extends AbstractStorageHelper {

  public static final String INSTANCES_URI = "/instance-storage/instances";
  public static final String MARC_JSON_RECORD_URI = "/instance-storage/instances/%s/source-record/marc-json";

  private static final String ID = "id";
  public static final String DISCOVERY_SUPPRESS = "discoverySuppress";

  /**
   *
   * @param entries the data returned by inventory-storage. The response of the /instance-storage/instances endpoint contains
   *                {@literal instances}
   * @return array of the items returned by inventory-storage
   */
  @Override
  public JsonArray getItems(JsonObject entries) {
    return entries.getJsonArray("instances");
  }

  /**
   * Returns id of the item
   * @param entry the item item returned by inventory-storage
   * @return id of the item
   */
  @Override
  public String getRecordId(JsonObject entry) {
    return entry.getString(ID);
  }

  @Override
  public String getIdentifierId(final JsonObject entry) {
    return getRecordId(entry);
  }

  @Override
  public String buildRecordsEndpoint(Request request, boolean recordsPath) throws UnsupportedEncodingException {
    return INSTANCES_URI + buildSearchQuery(request);
  }

  /**
   * Gets endpoint to search for record metadata by identifier
   * @param id instance identifier
   * @return endpoint to get metadata by identifier
   */
  @Override
  public String getRecordByIdEndpoint(String id){
    return String.format(MARC_JSON_RECORD_URI, id);
  }

  @Override
  public String getInstanceRecordSource(JsonObject entry) {
    return null;
  }

  @Override
  public String getRecordSource(JsonObject record) {
    return record.toString();
  }

  @Override
  protected void addSource(CQLQueryBuilder queryBuilder) {
    queryBuilder.addStrictCriteria("sourceRecordFormat", "MARC-JSON");
  }

  @Override
  void addSuppressFromDiscovery(final CQLQueryBuilder queryBuilder) {

  }

  @Override
  protected String getIdentifierName() {
    return ID;
  }

  @Override
  public boolean getSuppressedFromDiscovery(final JsonObject entry) {
    return entry.getBoolean(DISCOVERY_SUPPRESS);
  }

  @Override
  public boolean isRecordMarkAsDeleted(JsonObject entry) {
    return false;
  }

  @Override
  public JsonArray getRecordsItems(JsonObject entries) {
    return getItems(entries);
  }

  @Override
  public String getId(JsonObject entry) {
    return getRecordId(entry);
  }
}
