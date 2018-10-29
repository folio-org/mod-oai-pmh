package org.folio.oaipmh.helpers;

import jersey.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.text.StrSubstitutor;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

/**
 * Utility class used to build http CQL query param.
 */
public class CQLQueryBuilder {

  /** CQL query statement to search records by source. */
  private static final String SOURCE_STATEMENT = "source==%s";
  /** CQL query statement to search records created/updated within date range. */
  private static final String DATE_RANGE_STATEMENT =
    "((metadata.updatedDate==null and metadata.createdDate>%{from} and metadata.createdDate<%{until}) " +
      "or (metadata.updatedDate<>null and metadata.updatedDate>%{from} and metadata.updatedDate<%{until}))";

  private StringBuilder builder = new StringBuilder();
  private String prefix;

  public CQLQueryBuilder() {
    this(true);
  }

  public CQLQueryBuilder(boolean addQuestionMark) {
    prefix = addQuestionMark ? "?query=" : "&query=";
  }

  public CQLQueryBuilder source(String source) {
    builder.append(String.format(SOURCE_STATEMENT, source));
    return this;
  }

  public CQLQueryBuilder dateRange(String from, String until) {
    Map<String, String> valueMap = ImmutableMap.of("from", from, "until", until);
    StrSubstitutor sub = new StrSubstitutor(valueMap, "%{", "}");
    builder.append(sub.replace(DATE_RANGE_STATEMENT));
    return this;
  }

  public CQLQueryBuilder and() {
    builder.append(" and ");
    return this;
  }

  public String build() throws UnsupportedEncodingException {
    return prefix + URLEncoder.encode(builder.toString(), "UTF-8");
  }
}
