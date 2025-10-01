package org.folio.oaipmh.helpers.storage;

import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;
import static org.folio.oaipmh.Constants.CONTENT;
import static org.folio.oaipmh.Constants.INSTANCE_ID_FROM_VIEW_RESPONSE;
import static org.folio.oaipmh.Constants.MARC_RECORD_FROM_VIEW_RESPONSE;
import static org.folio.oaipmh.Constants.PARSED_RECORD;
import static org.folio.oaipmh.Constants.TOTAL_RECORDS_PARAM;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.text.ParseException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RecordStorageHelper implements StorageHelper {

  protected final Logger logger = LogManager.getLogger(getClass());

  private static final String[] patterns = {"yyyy-MM-dd'T'HH:mm:ss.SSSZ",
      "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "yyyy-MM-dd'T'HH:mm'Z'"};
  private static final String RECORD_ID = "recordId";
  private static final String ID = "id";
  private static final String LEADER = "leader";
  private static final String DELETED = "deleted";
  private static final String INSTANCE_ID = "instanceId";
  private static final String EXTERNAL_IDS_HOLDER = "externalIdsHolder";
  private static final String ADDITIONAL_INFO = "additionalInfo";
  private static final String SUPPRESS_DISCOVERY = "suppressDiscovery";
  private static final String RECORD = "record";

  @Override  public Integer getTotalRecords(JsonObject entries) {
    return entries.getInteger(TOTAL_RECORDS_PARAM);
  }

  @Override
  public Instant getLastModifiedDate(JsonObject entry) {
    JsonObject metadata = ofNullable(entry.getJsonObject("metadata"))
        .orElse(entry.containsKey(RECORD)
            ? entry.getJsonObject(RECORD).getJsonObject("metadata") : null);
    Instant instant = Instant.EPOCH;
    String lastModifiedDate = nonNull(metadata)
        ? metadata.getString("updatedDate") : entry.getString("instance_updated_date");
    if (lastModifiedDate == null) {
      // According to metadata.schema the createdDate is required so it should be always available
      lastModifiedDate = nonNull(metadata)
          ? metadata.getString("createdDate") : entry.getString("instance_created_date");
    }
    if (lastModifiedDate != null) {
      try {
        instant = DateUtils.parseDateStrictly(lastModifiedDate, patterns).toInstant();
      } catch (ParseException parseException) {
        try {
          instant = Instant.parse(lastModifiedDate);
        } catch (DateTimeParseException dateTimeParseException) {
          logger.error("Unable to parse the last modified date.", dateTimeParseException);
          return instant.truncatedTo(ChronoUnit.SECONDS);
        }
      }
    }
    return instant.truncatedTo(ChronoUnit.SECONDS);
  }

  @Override
  public JsonArray getItems(JsonObject entries) {
    return ofNullable(entries.getJsonArray("sourceRecords"))
        .orElse(entries.getJsonArray("instances"));
  }

  @Override
  public JsonArray getRecordsItems(JsonObject entries) {
    return entries.getJsonArray("records");
  }

  @Override
  public String getRecordId(JsonObject entry) {
    if (entry.containsKey(INSTANCE_ID_FROM_VIEW_RESPONSE)) {
      return entry.getString(INSTANCE_ID_FROM_VIEW_RESPONSE);
    }
    return ofNullable(entry.getString(RECORD_ID)).orElse(entry.getString(ID));
  }

  /**
   * Returns instance id that is linked to record within externalIdsHolder field.
   *
   * @param entry the item returned by source-storage or inventory-storage
   * @return instance id
   */
  @Override
  public String getIdentifierId(final JsonObject entry) {
    if (entry.containsKey(INSTANCE_ID_FROM_VIEW_RESPONSE)) {
      return entry.getString(INSTANCE_ID_FROM_VIEW_RESPONSE);
    }
    Optional<JsonObject> jsonObject = ofNullable(entry.getJsonObject(EXTERNAL_IDS_HOLDER));
    return jsonObject.map(obj -> obj.getString(INSTANCE_ID))
      .orElse(ofNullable(entry.getString(ID)).orElse(""));
  }

  @Override
  public String getInstanceRecordSource(JsonObject entry) {
    return ofNullable(entry.getJsonObject(PARSED_RECORD))
      .map(jsonRecord -> jsonRecord.getJsonObject(CONTENT))
      .map(JsonObject::encode)
      .orElse(null);
  }

  @Override
  public String getRecordSource(JsonObject entry) {
    return getInstanceRecordSource(entry);
  }

  @Override
  public boolean getSuppressedFromDiscovery(final JsonObject entry) {
    if (entry.containsKey("source")) {
      Boolean res;
      if (entry.getString("source").contains("MARC")
          && nonNull(entry.getString(MARC_RECORD_FROM_VIEW_RESPONSE))) {
        res = entry.getBoolean("suppress_from_discovery_srs");
      } else {
        res = entry.getBoolean("suppress_from_discovery_inventory");
      }
      if (nonNull(res)) {
        return res;
      }
    }
    if (entry.containsKey(RECORD)) {
      return entry.getJsonObject(RECORD).getBoolean("discoverySuppress");
    }
    Optional<JsonObject> jsonObject = ofNullable(entry.getJsonObject(ADDITIONAL_INFO));
    return jsonObject.map(obj -> obj.getBoolean(SUPPRESS_DISCOVERY))
      .orElse(entry.getBoolean("discoverySuppress"));
  }

  private String getLeaderValue(JsonObject entry) {
    return ofNullable(entry.getJsonObject(PARSED_RECORD))
        .map(jsonRecord -> jsonRecord.getJsonObject(CONTENT))
        .map(content -> content.getString(LEADER))
        .orElse("");
  }

  private boolean isLeaderValueContainsDeletedFlag(String leaderValue) {
    final String leaderDeletedRegexp = "\\d{5}d";
    final Pattern pattern = Pattern.compile(leaderDeletedRegexp);
    if (leaderValue != null) {
      return pattern.matcher(leaderValue).find();
    }
    return false;
  }

  private boolean getDeletedValue(JsonObject entry) {
    return ofNullable(entry.getValue(DELETED))
        .map(value -> Boolean.parseBoolean(value.toString())).orElse(false);
  }

  @Override
  public boolean isRecordMarkAsDeleted(JsonObject entry) {
    return isLeaderValueContainsDeletedFlag(getLeaderValue(entry)) || getDeletedValue(entry);
  }

  @Override
  public String getId(JsonObject entry) {
    return entry.getString(ID);
  }

}
