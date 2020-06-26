package org.folio.oaipmh.helpers.storage;

import static org.folio.oaipmh.Constants.CONTENT;
import static org.folio.oaipmh.Constants.PARSED_RECORD;

import java.util.Optional;
import java.util.regex.Pattern;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class SourceRecordStorageHelper extends AbstractStorageHelper {

  private static final String RECORD_ID = "recordId";
  private static final String ID = "id";
  private static final String LEADER = "leader";
  private static final String DELETED = "deleted";

  private static final String INSTANCE_ID = "instanceId";
  private static final String EXTERNAL_IDS_HOLDER = "externalIdsHolder";
  private static final String ADDITIONAL_INFO = "additionalInfo";
  private static final String SUPPRESS_DISCOVERY = "suppressDiscovery";

  @Override
  public JsonArray getItems(JsonObject entries) {
    return entries.getJsonArray("sourceRecords");
  }

  @Override
  public JsonArray getRecordsItems(JsonObject entries) {
    return entries.getJsonArray("records");
  }

  @Override
  public String getRecordId(JsonObject entry) {
    return entry.getString(RECORD_ID);
  }

  /**
   * Returns instance id that is linked to record within externalIdsHolder field.
   *
   * @param entry the item returned by source-storage
   * @return instance id
   */
  @Override
  public String getIdentifierId(final JsonObject entry) {
    Optional<JsonObject> jsonObject = Optional.ofNullable(entry.getJsonObject(EXTERNAL_IDS_HOLDER));
    return jsonObject.map(obj -> obj.getString(INSTANCE_ID))
      .orElse("");
  }

  @Override
  public String getInstanceRecordSource(JsonObject entry) {
    return Optional.ofNullable(entry.getJsonObject(PARSED_RECORD))
      .map(record -> record.getJsonObject(CONTENT))
      .map(JsonObject::encode)
      .orElse(null);
  }

  @Override
  public String getRecordSource(JsonObject record) {
    return getInstanceRecordSource(record);
  }

  @Override
  public boolean getSuppressedFromDiscovery(final JsonObject entry) {
    JsonObject jsonObject = entry.getJsonObject(ADDITIONAL_INFO);
    return jsonObject != null && jsonObject.getBoolean(SUPPRESS_DISCOVERY);
  }

  private String getLeaderValue(JsonObject entry) {
    return Optional.ofNullable(entry.getJsonObject(PARSED_RECORD))
      .map(record -> record.getJsonObject(CONTENT))
      .map(content -> content.getString(LEADER))
      .orElse("");
  }

  private boolean isLeaderValueContainsDeletedFlag(String leaderValue) {
    final String leaderDeletedRegexp = "\\d{5}[dsx]";
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
