package org.folio.oaipmh.helpers;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.oaipmh.Request;

import java.io.UnsupportedEncodingException;
import java.time.Instant;

public interface InstancesStorageHelper {

  static InstancesStorageHelper getStorageHelper() {
    // At the moment we are working with inventory storage services so creating corresponding service by default.
    return new InventoryStorageHelper();
  }

  /**
   * Extracts array of the items from {@linkplain JsonObject entries}
   * @param entries the data returned by items storage service
   * @return array of the items returned by inventory-storage
   */
  JsonArray getItems(JsonObject entries);

  /**
   * Returns item's last modified date or if no such just created date
   * @param item the item item returned by items storage service
   * @return {@link Instant} based on updated or created date
   */
  Instant getLastModifiedDate(JsonObject item);

  /**
   * Returns id of the item
   * @param item the item item returned by items storage service
   * @return {@link Instant} based on updated or created date
   */
  String getItemId(JsonObject item);

  /**
   * Returns base endpoint to get items
   * @return endpoint
   */
  String buildItemsEndpoint(Request request) throws UnsupportedEncodingException;

  /**
   * Gets endpoint to search for an instance by identifier
   * @param id instance identifier
   * @return endpoint to search for an instance by identifier
   */
  String getInstanceEndpoint(String id);
}
