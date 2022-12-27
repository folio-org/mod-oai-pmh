package org.folio.oaipmh.helpers.storage;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class InventoryRecordsStorageHelper extends AbstractStorageHelper {

  @Override
  public JsonArray getItems(JsonObject entries) {
    return null;
  }

  @Override
  public String getRecordId(JsonObject entry) {
    return null;
  }

  @Override
  public String getIdentifierId(JsonObject entry) {
    return null;
  }

  @Override
  public String getInstanceRecordSource(JsonObject entry) {
    return null;
  }

  @Override
  public String getRecordSource(JsonObject record) {
    return null;
  }

  @Override
  public boolean getSuppressedFromDiscovery(JsonObject entry) {
    return false;
  }

  @Override
  public JsonArray getRecordsItems(JsonObject entries) {
    return null;
  }

  @Override
  public String getId(JsonObject entry) {
    return null;
  }

  @Override
  public boolean isRecordMarkAsDeleted(JsonObject entry) {
    return false;
  }
}
