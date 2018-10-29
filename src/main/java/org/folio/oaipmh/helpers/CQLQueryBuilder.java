package org.folio.oaipmh.helpers;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Utility class used to build http CQL query param.
 */
public class CQLQueryBuilder {

  private StringBuilder builder = new StringBuilder();
  private String prefix;

  public CQLQueryBuilder() {
    this(true);
  }

  public CQLQueryBuilder(boolean addQuestionMark) {
    prefix = addQuestionMark ? "?query=" : "&query=";
  }

  /**
   * Adds a statement to search by source to the query.
   *
   * @param source the source to search by
   * @return {@link CQLQueryBuilder}
   */
  public CQLQueryBuilder source(String source) {
    builder.append(String.format("source==%s", source));
    return this;
  }

  /**
   * Adds a statement to search by id to the query.
   *
   * @param id the id to search by
   * @return {@link CQLQueryBuilder}
   */
  public CQLQueryBuilder identifier(String id) {
    builder.append(String.format("id==%s", id));
    return this;
  }

  /**
   * Adds a statement to search by date range (or just lower/upper bound) to the query.
   *
   * @param from the lower date bound
   * @param until the upper date bound
   * @return {@link CQLQueryBuilder}
   */
  public CQLQueryBuilder dateRange(String from, String until) {
    builder
      .append("(")
      .append(String.format("(metadata.updatedDate==null%s%s)",
        isNotEmpty(from) ? String.format(" and metadata.createdDate>%s", from) : EMPTY,
        isNotEmpty(until) ? String.format(" and metadata.createdDate<%s", until) : EMPTY))
      .append(String.format(" or (metadata.updatedDate<>null%s%s)",
        isNotEmpty(from) ? String.format(" and metadata.updatedDate>%s", from) : EMPTY,
        isNotEmpty(until) ? String.format(" and metadata.updatedDate<%s", until) : EMPTY))
      .append(")");

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
