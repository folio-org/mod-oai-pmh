package org.folio.oaipmh.helpers.storage;

import static org.folio.oaipmh.Constants.TOTAL_RECORDS_PARAM;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import io.vertx.core.json.JsonObject;

public abstract class AbstractStorageHelper implements StorageHelper {

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

  @Override
  public Integer getTotalRecords(JsonObject entries) {
    return entries.getInteger(TOTAL_RECORDS_PARAM);
  }

  @Override
  public Instant getLastModifiedDate(JsonObject record) {
    // Get metadat described by ramls/raml-util/schemas/metadata.schema
    JsonObject metadata = record.getJsonObject("metadata");
    Instant datetime = Instant.EPOCH;
    if (metadata != null) {
      Optional<String> date = Optional.ofNullable(metadata.getString("updatedDate"));
      // According to metadata.schema the createdDate is required so it should be always available
      datetime = formatter.parse(date.orElseGet(() -> metadata.getString("createdDate")), Instant::from);
    }
    return datetime.truncatedTo(ChronoUnit.SECONDS);
  }

}
