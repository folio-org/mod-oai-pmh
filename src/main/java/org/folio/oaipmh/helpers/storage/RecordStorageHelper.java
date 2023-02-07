package org.folio.oaipmh.helpers.storage;

import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.regex.Pattern;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.folio.oaipmh.Constants.TOTAL_RECORDS_PARAM;
import static org.folio.oaipmh.Constants.PARSED_RECORD;
import static org.folio.oaipmh.Constants.CONTENT;

public class RecordStorageHelper implements StorageHelper {

  protected final Logger logger = LogManager.getLogger(getClass());

  private static final String[] patterns = {"yyyy-MM-dd'T'HH:mm:ss.SSSZ", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"};
  private static final String RECORD_ID = "recordId";
  private static final String ID = "id";
  private static final String LEADER = "leader";
  private static final String DELETED = "deleted";
  private static final String INSTANCE_ID = "instanceId";
  private static final String EXTERNAL_IDS_HOLDER = "externalIdsHolder";
  private static final String ADDITIONAL_INFO = "additionalInfo";
  private static final String SUPPRESS_DISCOVERY = "suppressDiscovery";

  @Override
  public Integer getTotalRecords(JsonObject entries) {
    return entries.getInteger(TOTAL_RECORDS_PARAM);
  }

  @Override
  public Instant getLastModifiedDate(JsonObject entry) {
    JsonObject metadata = entry.getJsonObject("metadata");
    Instant instant = Instant.EPOCH;
    if (metadata != null) {
      try {
        String lastModifiedDate = metadata.getString("updatedDate");
        if (lastModifiedDate == null) {
          // According to metadata.schema the createdDate is required so it should be always available
          lastModifiedDate = metadata.getString("createdDate");
        }
        instant = DateUtils.parseDateStrictly(lastModifiedDate, patterns).toInstant();
      } catch (ParseException parseException) {
        logger.error("Unable to parse the last modified date.", parseException);
        return instant.truncatedTo(ChronoUnit.SECONDS);
      }
    }
    return instant.truncatedTo(ChronoUnit.SECONDS);
  }

  @Override
  public JsonArray getItems(JsonObject entries) {
    return Optional.ofNullable(entries.getJsonArray("sourceRecords")).orElse(entries.getJsonArray("instances"));
  }

  @Override
  public JsonArray getRecordsItems(JsonObject entries) {
    return entries.getJsonArray("records");
  }

  @Override
  public String getRecordId(JsonObject entry) {
    return Optional.ofNullable(entry.getString(RECORD_ID)).orElse(entry.getString(ID));
  }

  /**
   * Returns instance id that is linked to record within externalIdsHolder field.
   *
   * @param entry the item returned by source-storage or inventory-storage
   * @return instance id
   */
  @Override
  public String getIdentifierId(final JsonObject entry) {
    Optional<JsonObject> jsonObject = Optional.ofNullable(entry.getJsonObject(EXTERNAL_IDS_HOLDER));
    return jsonObject.map(obj -> obj.getString(INSTANCE_ID))
      .orElse(Optional.ofNullable(entry.getString(ID)).orElse(""));
  }

  @Override
  public String getInstanceRecordSource(JsonObject entry) {
    return Optional.ofNullable(entry.getJsonObject(PARSED_RECORD))
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
    Optional<JsonObject> jsonObject = Optional.ofNullable(entry.getJsonObject(ADDITIONAL_INFO));
    return jsonObject.map(obj -> obj.getBoolean(SUPPRESS_DISCOVERY))
      .orElse(entry.getBoolean("discoverySuppress"));
  }

  private String getLeaderValue(JsonObject entry) {
    return Optional.ofNullable(entry.getJsonObject(PARSED_RECORD))
      .map(jsonRecord -> jsonRecord.getJsonObject(CONTENT))
      .map(content -> content.getString(LEADER))
      .orElse("");
  }

  private boolean isLeaderValueContainsDeletedFlag(String leaderValue) {
    final String leaderDeletedRegexp = "\\d{5}[d]";
    final Pattern pattern = Pattern.compile(leaderDeletedRegexp);
    if (leaderValue != null) {
      return pattern.matcher(leaderValue).find();
    }
    return false;
  }

  private boolean getDeletedValue(JsonObject entry) {
    return Optional.ofNullable(entry.getValue(DELETED))
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
