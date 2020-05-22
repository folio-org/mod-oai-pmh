package org.folio.oaipmh.domain;

import static com.google.common.collect.ImmutableSet.of;
import static java.util.Collections.EMPTY_SET;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.folio.oaipmh.Constants.FROM_PARAM;
import static org.folio.oaipmh.Constants.IDENTIFIER_PARAM;
import static org.folio.oaipmh.Constants.METADATA_PREFIX_PARAM;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_PARAM;
import static org.folio.oaipmh.Constants.SET_PARAM;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openarchives.oai._2.OAIPMHerrorType;

import io.vertx.ext.web.RoutingContext;

/**
 * Enum that represents OAI-PMH verbs with associated http parameters and validation logic.
 */
public enum Verb {
  GET_RECORD("GetRecord", of(IDENTIFIER_PARAM, METADATA_PREFIX_PARAM), EMPTY_SET, null),
  IDENTIFY("Identify", EMPTY_SET, EMPTY_SET, null),
  LIST_IDENTIFIERS("ListIdentifiers", of(METADATA_PREFIX_PARAM), of(FROM_PARAM, UNTIL_PARAM, SET_PARAM), RESUMPTION_TOKEN_PARAM),
  LIST_METADATA_FORMATS("ListMetadataFormats", EMPTY_SET, of(IDENTIFIER_PARAM), null),
  LIST_RECORDS("ListRecords", of(METADATA_PREFIX_PARAM), of(FROM_PARAM, UNTIL_PARAM, SET_PARAM), RESUMPTION_TOKEN_PARAM),
  LIST_SETS("ListSets", EMPTY_SET, EMPTY_SET, RESUMPTION_TOKEN_PARAM);
  //VERB("Verb", EMPTY_SET, EMPTY_SET, null); kek

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

  /** ISO Date and Time with UTC offset. */
  private static final DateTimeFormatter ISO_UTC_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
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

  /**
   * Checks if given {@link RoutingContext}'s http parameters are valid for current verb.
   *
   * @param ctx {@link RoutingContext} to validate
   * @return list of {@link OAIPMHerrorType} errors if any or empty list otherwise
   */
  public List<OAIPMHerrorType> validate(RoutingContext ctx) {
    List<OAIPMHerrorType> errors = new ArrayList<>();

    validateIllegalParams(ctx, errors);
    validateExclusiveParam(ctx, errors);

    if (ctx.request().getParam(FROM_PARAM) != null) {
      validateDatestamp(FROM_PARAM, ctx.request().getParam(FROM_PARAM), errors);
    }
    if (ctx.request().getParam(UNTIL_PARAM) != null) {
      validateDatestamp(UNTIL_PARAM, ctx.request().getParam(UNTIL_PARAM), errors);
    }

    return errors;
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


  private void validateIllegalParams(RoutingContext ctx, List<OAIPMHerrorType> errors) {
    ctx.request().params().entries().stream()
      .map(Map.Entry::getKey)
      .filter(param -> !allParams.contains(param))
      .forEach(param -> errors.add(new OAIPMHerrorType()
        .withCode(BAD_ARGUMENT)
        .withValue("Verb '" + name + "', illegal argument: " + param)));
  }

  private void validateExclusiveParam(RoutingContext ctx, List<OAIPMHerrorType> errors) {
    if (exclusiveParam != null && ctx.request().getParam(exclusiveParam) != null) {
      ctx.request().params().entries().stream()
        .map(Map.Entry::getKey)
        .filter(p -> !excludeParams.contains(p))
        .filter(p -> !exclusiveParam.equals(p))
        .findAny()
        .ifPresent(param -> errors.add(new OAIPMHerrorType()
          .withCode(BAD_ARGUMENT)
          .withValue("Verb '" + name + "', argument '" + exclusiveParam +
            "' is exclusive, no others maybe specified with it.")));
    }
  }

  private void validateDatestamp(String paramName, String paramValue, List<OAIPMHerrorType> errors) {
    try {
      LocalDateTime.parse(paramValue, ISO_UTC_DATE_TIME);
    } catch (DateTimeParseException e) {
      // The repository must support YYYY-MM-DD granularity so try to parse date only
      try {
        LocalDate.parse(paramValue);
      } catch (DateTimeParseException ex) {
        errors.add(new OAIPMHerrorType()
          .withCode(BAD_ARGUMENT)
          .withValue("Bad datestamp format for '" + paramName + "' argument."));
      }
    }
  }
}

