package org.folio.oaipmh.helpers.storage;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.ISO_UTC_DATE_TIME;

/**
 * Utility class used to build http CQL query param.
 *
 * Deprecated because SRS no longer supports CQL
 */
@Deprecated
public class CQLQueryBuilder {

  /**
   * According to ramls/raml-util/schemas/metadata.schema the dates' "format": "date-time"
   * Limiting query to ISO 8601 date and time with milliseconds and UTC offset.
   */
  private final DateTimeFormatter queryDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  private StringBuilder builder = new StringBuilder();
  private String prefix;

  public CQLQueryBuilder() {
    this(true);
  }

  public CQLQueryBuilder(boolean addQuestionMark) {
    prefix = addQuestionMark ? "?query=" : "&query=";
  }

  /**
   * Adds a statement to search by key with strict value to the query.
   *
   * @param key the condition to search by
   * @param value the value of the condition to search by
   * @return {@link CQLQueryBuilder}
   */
  public CQLQueryBuilder addStrictCriteria(String key, String value) {
    builder.append(String.format("%s==%s", key, value));
    return this;
  }

  /**
   * Adds a statement to search by date range (or just lower/upper bound) to the query.
   * The query is being updated with metadata.updatedDate checks only.
   *
   * @param from the lower date bound
   * @param until the upper date bound
   * @return {@link CQLQueryBuilder}
   */
  public CQLQueryBuilder dateRange(String from, String until) {
    boolean notEmptyUntil = isNotEmpty(until);

    if (isNotEmpty(from)) {
      builder.append(String.format("metadata.updatedDate>=%s", formatDateTime(from, false)));

      if (notEmptyUntil) {
        this.and();
      }
    }

    if (notEmptyUntil) {
      // The until param is inclusive. Due to the difference of OAI-PMH and storage granularity, the until will be incremented by one and compared as exclusive
      builder.append(String.format("metadata.updatedDate<%s", formatDateTime(until, true)));
    }

    return this;
  }

  /**
   * OAI-PMH finest granularity is by second but storage granularity is by millisecond. According to ramls/raml-util/schemas/metadata.schema
   * the format of the dates is "date-time" which is ISO 8601 date and time with milliseconds and UTC offset. So the date should be
   * formatted to include milliseconds as well.
   */
  private String formatDateTime(String date, boolean isUntil) {
    return queryDateFormat.format(parseDateTime(date, isUntil));
  }

  /**
   * Converts passed date stamp string to LocalDateTime. In case this is until date, it is being incremented by one second.
   * In case passed date stamp is in date only format {@link #parseDate(String, boolean)} will be called.
   */
  private LocalDateTime parseDateTime(String date, boolean isUntil) {
    LocalDateTime dateTime;
    try {
      dateTime = LocalDateTime.parse(date, ISO_UTC_DATE_TIME);
    } catch (DateTimeParseException e) {
      // The repository must support YYYY-MM-DD granularity so try to parse date only
      return parseDate(date, isUntil);
    }
    if (isUntil) {
      dateTime = dateTime.plusSeconds(1L);
    }
    return dateTime;
  }

  /**
   * Converts passed date string to LocalDateTime at time of midnight. In case this is until date, it is being incremented by one day.
   */
  private LocalDateTime parseDate(String date, boolean isUntil) {
    LocalDateTime dateTime = LocalDate.parse(date).atStartOfDay();
    if (isUntil) {
      dateTime = dateTime.plusDays(1L);
    }
    return dateTime;
  }

  public CQLQueryBuilder and() {
    builder.append(" and ");
    return this;
  }

  public String build() throws UnsupportedEncodingException {
    return prefix + URLEncoder.encode(builder.toString(), "UTF-8");
  }
}
