package org.folio.oaipmh.domain;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.EMPTY_SET;
import static java.util.Set.of;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.folio.oaipmh.Constants.*;

/**
 * Enum that represents OAI-PMH verbs with associated http parameters and validation logic.
 */
public enum Verb {
  GET_RECORD("GetRecord", of(IDENTIFIER_PARAM, METADATA_PREFIX_PARAM), EMPTY_SET, null),
  IDENTIFY("Identify", EMPTY_SET, EMPTY_SET, null),
  LIST_IDENTIFIERS("ListIdentifiers", of(METADATA_PREFIX_PARAM), of(FROM_PARAM, UNTIL_PARAM, SET_PARAM, EXPIRATION_DATE_RESUMPTION_TOKEN_PARAM), RESUMPTION_TOKEN_PARAM),
  LIST_METADATA_FORMATS("ListMetadataFormats", EMPTY_SET, of(IDENTIFIER_PARAM), null),
  LIST_RECORDS("ListRecords", of(METADATA_PREFIX_PARAM), of(FROM_PARAM, UNTIL_PARAM, SET_PARAM, EXPIRATION_DATE_RESUMPTION_TOKEN_PARAM), RESUMPTION_TOKEN_PARAM),
  LIST_SETS("ListSets", EMPTY_SET, EMPTY_SET, RESUMPTION_TOKEN_PARAM);

  private static final String VERB_PARAM = "verb";
  private static final String PARAM_API_KEY = "apikey";
  private static final String PATH_API_KEY = "apiKeyPath";

  /** String name of the verb. */
  private String name;
  /** Required http parameters associated with the verb. */
  private Set<String> requiredParams;
  /** Optional http parameters associated with the verb. */
  private Set<String> optionalParams;
  /** Exclusive (i.e. must be the only one if provided) http parameter associated with the verb. */
  private String exclusiveParam;
  /** All possible parameters associated with the verb. */
  private Set<String> allParams;

  /** Parameters which must be excluded from validation if present in http request. */
  private final Set<String> excludeParams = of(VERB_PARAM, PARAM_API_KEY, PATH_API_KEY);

  private static final Map<String, Verb> CONSTANTS = new HashMap<>();
  static {
    for (Verb v : values())
      CONSTANTS.put(v.name, v);
  }


  Verb(String name, Set<String> requiredParams, Set<String> optionalParams, String exclusiveParam) {
    this.name = name;
    this.requiredParams = requiredParams;
    this.optionalParams = optionalParams;
    this.exclusiveParam = exclusiveParam;

    allParams = concat(concat(requiredParams.stream(), optionalParams.stream()), excludeParams.stream())
      .collect(toSet());
    allParams.add(exclusiveParam);
  }

  public static Verb fromName(String name) {
    return CONSTANTS.get(name);
  }

  public Set<String> getRequiredParams() {
    return requiredParams;
  }

  public Set<String> getOptionalParams() {
    return optionalParams;
  }

  public Set<String> getExcludedParams() {
    return excludeParams;
  }

  public String getExclusiveParam() {
    return exclusiveParam;
  }

  public Set<String> getAllParams() {
    return allParams;
  }

  @Override
  public String toString() {
    return this.name;
  }
}

