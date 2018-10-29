package org.folio.oaipmh.helpers;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.oaipmh.Request;

import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class InventoryStorageHelper implements InstancesStorageHelper {

  private static final String RECORDS_LIMIT = "100";

  /**
   * The dates returned by inventory storage service are in format "2018-09-19T02:52:08.873+0000".
   * Using {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME} and just in case 2 offsets "+HHmm" and "+HH:MM"
   */
  private static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    .optionalStart().appendOffset("+HH:MM", "Z").optionalEnd()
    .optionalStart().appendOffset("+HHmm", "Z").optionalEnd()
    .toFormatter();

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
   * Returns item's last modified date or if no such just created date
   * @param item the item item returned by inventory-storage
   * @return {@link Instant} based on updated or created date
   */
  @Override
  public Instant getLastModifiedDate(JsonObject item) {
    // Get metadat described by ramls/raml-util/schemas/metadata.schema
    JsonObject metadata = item.getJsonObject("metadata");
    Instant datetime = Instant.EPOCH;
    if (metadata != null) {
      Optional<String> date = Optional.ofNullable(metadata.getString("updatedDate"));
      // According to metadata.schema the createdDate is required so it should be always available
      datetime = formatter.parse(date.orElseGet(() -> metadata.getString("createdDate")), Instant::from);
    }
    return datetime.truncatedTo(ChronoUnit.SECONDS);
  }

  /**
   * Returns id of the item
   * @param item the item item returned by inventory-storage
   * @return id of the item
   */
  @Override
  public String getItemId(JsonObject item) {
    return item.getString("id");
  }

  @Override
  public String buildItemsEndpoint(Request request) throws UnsupportedEncodingException {
    CQLQueryBuilder queryBuilder = new CQLQueryBuilder();
    queryBuilder.source("MARC");
    if (isNotEmpty(request.getFrom())) {
      queryBuilder
        .and()
        .dateRange(request.getFrom(), request.getUntil());
    }

    return "/instance-storage/instances" + queryBuilder.build() + "&limit=" + RECORDS_LIMIT;
  }

  @Override
  public String getInstanceEndpoint(String id) {
    return "/instance-storage/instances?query=source==MARC+and+id=" + id;
  }
}
