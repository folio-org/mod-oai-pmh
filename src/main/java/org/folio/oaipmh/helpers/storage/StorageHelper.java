package org.folio.oaipmh.helpers.storage;

import static org.folio.oaipmh.Constants.INVENTORY_STORAGE;
import static org.folio.oaipmh.Constants.REPOSITORY_STORAGE;
import static org.folio.oaipmh.Constants.SOURCE_RECORD_STORAGE;

import java.io.UnsupportedEncodingException;
import java.time.Instant;

import org.folio.oaipmh.Request;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public interface StorageHelper {

  /**
   * Creates instance of the StorageHelper depending on the `repository.storage` system property.
   */
  static StorageHelper getInstance() {
    String repositoryType = System.getProperty(REPOSITORY_STORAGE, SOURCE_RECORD_STORAGE);
    if (SOURCE_RECORD_STORAGE.equals(repositoryType)) {
      return new SourceRecordStorageHelper();
    } else if (INVENTORY_STORAGE.equals(repositoryType)) {
      return new InventoryStorageHelper();
    } else {
      throw new UnsupportedOperationException(repositoryType + " repository is not supported.");
    }
  }

  /**
   * Extracts array of the items from {@linkplain JsonObject entries}
   * @param entries the data returned by items storage service
   * @return array of the items returned by inventory-storage
   */
  JsonArray getItems(JsonObject entries);

  /**
   * Extracts total number of records from {@linkplain JsonObject entries}
   * @param entries the data returned by items storage service
   * @return total number of records
   */
  Integer getTotalRecords(JsonObject entries);

  /**
   * Returns item's last modified date or if no such just created date
   * @param record the record returned by storage service
   * @return {@link Instant} based on updated or created date
   */
  Instant getLastModifiedDate(JsonObject record);

  /**
   * Returns id of the entry
   * @param entry the entry entry returned by items storage service
   * @return {@link Instant} based on updated or created date
   */
  String getRecordId(JsonObject entry);

  /**
   * Returns instance id that is used for building record identifier.
   * @param entry the entry entry returned by items storage service
   * @return inventory instance id
   */
  String getIdentifierId(JsonObject entry);

  /**
   * Returns the record's source of the instance returned in the list instances response
   * @param entry the instance returned by records storage service
   * @return {@link Instant} based on updated or created date
   */
  String getInstanceRecordSource(JsonObject entry);

  /**
   * Returns the source of the record
   * @param record the record returned by records storage service
   * @return {@link Instant} based on updated or created date
   */
  String getRecordSource(JsonObject record);

  /**
   * Returns base endpoint to get items
   * @return endpoint
   */
  String buildRecordsEndpoint(Request request) throws UnsupportedEncodingException;

  /**
   * Gets endpoint to search for record metadata by identifier
   * @param id instance identifier
   * @return endpoint to get metadata by identifier
   */
  String getRecordByIdEndpoint(String id);

  /**
   * Returns value that describes whether instance is suppressed from discovery
   * @param entry - the entry returned by items storage service
   * @return String
   */
  boolean getSuppressedFromDiscovery(JsonObject entry);
}
