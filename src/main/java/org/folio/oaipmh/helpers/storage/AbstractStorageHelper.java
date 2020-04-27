package org.folio.oaipmh.helpers.storage;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getProperty;

import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import org.folio.oaipmh.Request;

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
    return entries.getInteger("totalRecords");
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

  protected String buildSearchQuery(Request request) throws UnsupportedEncodingException {
    CQLQueryBuilder queryBuilder = new CQLQueryBuilder();
    addSource(queryBuilder);
    if(!getBooleanProperty(request, REPOSITORY_SUPPRESSED_RECORDS_PROCESSING)) {
      queryBuilder.and();
      addSuppressFromDiscovery(queryBuilder);
    }
    if (isNotEmpty(request.getIdentifier())) {
      queryBuilder
        .and()
        .addStrictCriteria(getIdentifierName(), request.getStorageIdentifier());
    } else if (isNotEmpty(request.getFrom()) || isNotEmpty(request.getUntil())) {
      queryBuilder
        .and()
        .dateRange(request.getFrom(), request.getUntil());
    }

    // one extra record is required to check if resumptionToken is good
    int limit = Integer.parseInt(getProperty(request.getOkapiHeaders().get(OKAPI_TENANT), REPOSITORY_MAX_RECORDS_PER_RESPONSE)) + 1;
    return queryBuilder.build()
      + "&limit=" + limit
      + "&offset=" + request.getOffset();
  }

  abstract String getIdentifierName();
  abstract void addSource(CQLQueryBuilder queryBuilder);
  abstract void addSuppressFromDiscovery(CQLQueryBuilder queryBuilder);
}
