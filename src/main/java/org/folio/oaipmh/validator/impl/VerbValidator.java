package org.folio.oaipmh.validator.impl;

import static java.lang.String.format;
import static org.folio.oaipmh.Constants.REQUEST_PARAMS;
import static org.folio.oaipmh.Constants.VERB_PARAM;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_VERB;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.domain.Verb;
import org.folio.oaipmh.validator.Validator;
import org.openarchives.oai._2.OAIPMHerrorType;

import io.vertx.core.Context;

public class VerbValidator implements Validator {

  private static final String VERB_NOT_IMPLEMENTED_ERROR_MESSAGE = "Bad verb. Verb \'%s\' is not implemented";
  private static final String EXCLUSIVE_PARAM_ERROR_MESSAGE = "Verb '%s', argument '%s' is exclusive, no others maybe specified with it.";
  private static final String MISSING_REQUIRED_PARAMETERS_ERROR_MESSAGE = "Missing required parameters: %s";
  private static final String ILLEGAL_PARAM_ERROR_MESSAGE = "Verb '%s', illegal argument: %s";

  /**
   * Validates request parameters except 'from' and 'until' against particular verb.
   *
   * @param object - anme of verb against witch parameters are validated
   * @param context - vertx context
   * @return list of errors.
   */
  @Override
  public List<OAIPMHerrorType> validate(Object object, Context context) {
    List<OAIPMHerrorType> errors = new ArrayList<>();
    String verbName = Objects.nonNull(object) ? object.toString() : "empty";
    Verb verb = Verb.fromName(verbName);
    if (Objects.nonNull(verb)) {
      validateRequiredParams(context, verb, errors);
      validateExclusiveParam(verb, context, errors);
      validateIllegalParams(verb, context, errors);
    } else {
      errors.add(new OAIPMHerrorType().withCode(BAD_VERB)
        .withValue(format(VERB_NOT_IMPLEMENTED_ERROR_MESSAGE, verbName)));
    }
    return errors;
  }

  /**
   * In case of resumption token param presence verifies if there any other parameters were specified too. If they were then error
   * will be added to error list.
   *
   * @param verb   - verb
   * @param ctx    - vertx context
   * @param errors - list of errors
   */
  private void validateExclusiveParam(Verb verb, Context ctx, List<OAIPMHerrorType> errors) {
    Map<String, String> requestParams = ctx.get(REQUEST_PARAMS);
    if (verb.getExclusiveParam() != null && requestParams.get(verb.getExclusiveParam()) != null) {
      requestParams.keySet()
        .stream()
        .filter(p -> !verb.getExcludedParams().contains(p))
        .filter(p -> !verb.getExclusiveParam().equals(p))
        .findAny()
        .ifPresent(param -> {
          if (!param.equals(VERB_PARAM)) {
            errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT)
              .withValue(format(EXCLUSIVE_PARAM_ERROR_MESSAGE, verb.name(), verb.getExclusiveParam())));
          }
        });
    }
  }

  /**
   * Verifies that any of the required parameters is not missing.
   *
   * @param context - vertx context
   * @param verb    - request verb
   * @param errors  - errors list
   */
  private void validateRequiredParams(Context context, Verb verb, List<OAIPMHerrorType> errors) {
    Map<String, String> requestParams = context.get(REQUEST_PARAMS);
    Set<String> params = new HashSet<>();

    if (verb.getExclusiveParam() != null && requestParams.get(verb.getExclusiveParam()) != null) {
      params.add(verb.getExclusiveParam());
    } else {
      params.addAll(verb.getRequiredParams());
    }

    final String missingRequiredParams = params.stream()
      .filter(p -> StringUtils.isEmpty(requestParams.get(p)))
      .collect(Collectors.joining(","));

    if (StringUtils.isNotEmpty(missingRequiredParams)) {
      errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT)
        .withValue(format(MISSING_REQUIRED_PARAMETERS_ERROR_MESSAGE, missingRequiredParams)));
    }
  }

  /**
   * Verifies if there no any illegal parameters were specified.
   *
   * @param verb   - verb
   * @param ctx    - vertx context
   * @param errors - list of errors
   */
  private void validateIllegalParams(Verb verb, Context ctx, List<OAIPMHerrorType> errors) {
    Map<String, String> requestParams = ctx.get(REQUEST_PARAMS);
    requestParams.keySet()
      .stream()
      .filter(param -> !verb.getAllParams()
        .contains(param))
      .forEach(param -> errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT)
        .withValue(format(ILLEGAL_PARAM_ERROR_MESSAGE, verb.name(), param))));
  }

}
