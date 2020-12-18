package org.folio.oaipmh.helpers.storage;

import static org.folio.oaipmh.Constants.TOTAL_RECORDS_PARAM;

import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.apache.commons.lang.time.DateUtils;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public abstract class AbstractStorageHelper implements StorageHelper {
  protected final Logger logger = LoggerFactory.getLogger(getClass());

  private static final String[] patterns = {"yyyy-MM-dd'T'HH:mm:ss.SSSZ", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"};

  @Override
  public Integer getTotalRecords(JsonObject entries) {
    return entries.getInteger(TOTAL_RECORDS_PARAM);
  }

  @Override
  public Instant getLastModifiedDate(JsonObject record) {
    JsonObject metadata = record.getJsonObject("metadata");
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
        logger.error("Unable to parse the last modified date", parseException);
        return instant.truncatedTo(ChronoUnit.SECONDS);
      }
    }
    return instant.truncatedTo(ChronoUnit.SECONDS);
  }
}
