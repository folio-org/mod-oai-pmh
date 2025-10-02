package org.folio.oaipmh.validator;

import static java.lang.String.format;
import static org.folio.oaipmh.Constants.EXPIRED_RESUMPTION_TOKEN;
import static org.folio.oaipmh.Constants.INVALID_RESUMPTION_TOKEN;
import static org.folio.oaipmh.Constants.VERB_PARAM;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_VERB;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.domain.Verb;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.springframework.stereotype.Component;

@Component
public class VerbValidator {

  private static final String VERB_NOT_IMPLEMENTED_ERROR_MESSAGE =
      "Bad verb. Verb \'%s\' is not implemented";
  private static final String EXCLUSIVE_PARAM_ERROR_MESSAGE =
      "Verb '%s', argument '%s' is exclusive, no others maybe specified with it.";
  private static final String MISSING_REQUIRED_PARAMETERS_ERROR_MESSAGE =
      "Missing required parameters: %s";
  private static final String ILLEGAL_PARAM_ERROR_MESSAGE = "Verb '%s', illegal argument: %s";

  /**
   * Validates request parameters except 'from' and 'until' against particular verb.
   *
   * @param object        - name of verb against witch parameters are validated
   * @param requestParams - map with request parameters
   * @return list of errors.
   */
  public List<OAIPMHerrorType> validate(Object object, Map<String, String> requestParams,
      Request request) {
    List<OAIPMHerrorType> errors = new ArrayList<>();
    String verbName = Objects.nonNull(object) ? object.toString() : "empty";
    Verb verb = Verb.fromName(verbName);
    if (Objects.nonNull(verb)) {
      validateRequiredParams(requestParams, verb, errors);
      validateExclusiveParam(verb, requestParams, request, errors);
      validateIllegalParams(verb, requestParams, errors);
    } else {
      errors.add(new OAIPMHerrorType().withCode(BAD_VERB)
          .withValue(format(VERB_NOT_IMPLEMENTED_ERROR_MESSAGE, verbName)));
    }
    return errors;
  }

  /**
   * In case of resumption token param presence verifies if there any other parameters were
   * specified too. If they were then error will be added to error list.
   *
   * @param verb          - verb
   * @param requestParams - request parameters
   * @param request       - oai-pmh request
   * @param errors        - list of errors
   */
  private void validateExclusiveParam(Verb verb, Map<String, String> requestParams,
      Request request, List<OAIPMHerrorType> errors) {
    String resumptionToken = requestParams.get(verb.getExclusiveParam());
    if (verb.getExclusiveParam() != null && resumptionToken != null) {
      requestParams.keySet()
          .stream()
          .filter(p -> !verb.getExcludedParams()
            .contains(p))
          .filter(p -> !verb.getExclusiveParam()
            .equals(p))
          .findAny()
          .ifPresent(param -> {
            if (!param.equals(VERB_PARAM)) {
              errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT)
                  .withValue(format(EXCLUSIVE_PARAM_ERROR_MESSAGE, verb.name(),
                      verb.getExclusiveParam())));
            }
          });
      if (!request.isResumptionTokenParsableAndValid()) {
        OAIPMHerrorType error = new OAIPMHerrorType()
            .withCode(BAD_RESUMPTION_TOKEN)
            .withValue(format(INVALID_RESUMPTION_TOKEN, verb.name()));
        errors.add(error);
        return;
      }
      if (request.isResumptionTokenTimeExpired()) {
        OAIPMHerrorType errorByExpiredTime = new OAIPMHerrorType()
            .withCode(BAD_RESUMPTION_TOKEN)
            .withValue(EXPIRED_RESUMPTION_TOKEN);
        errors.add(errorByExpiredTime);
      }
    }
  }

  /**
   * Verifies that any of the required parameters is not missing.
   *
   * @param requestParams - vertx context
   * @param verb          - request verb
   * @param errors        - errors list
   */
  private void validateRequiredParams(Map<String, String> requestParams, Verb verb,
      List<OAIPMHerrorType> errors) {
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
  private void validateIllegalParams(Verb verb, Map<String, String> requestParams,
      List<OAIPMHerrorType> errors) {
    requestParams.keySet()
        .stream()
        .filter(param -> !verb.getAllParams()
          .contains(param))
        .forEach(param -> errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT)
          .withValue(format(ILLEGAL_PARAM_ERROR_MESSAGE, verb.name(), param))));
  }

}
