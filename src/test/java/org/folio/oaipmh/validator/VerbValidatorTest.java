package org.folio.oaipmh.validator.impl;

import static java.lang.String.format;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.FROM_PARAM;
import static org.folio.oaipmh.Constants.IDENTIFIER_PARAM;
import static org.folio.oaipmh.Constants.ISO_UTC_DATE_ONLY;
import static org.folio.oaipmh.Constants.METADATA_PREFIX_PARAM;
import static org.folio.oaipmh.Constants.REQUEST_PARAMS;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_PARAM;
import static org.folio.oaipmh.Constants.SET_PARAM;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_VERB;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.domain.Verb;
import org.junit.After;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;

import com.google.common.collect.ImmutableList;

import io.vertx.core.Context;
import io.vertx.core.impl.EventLoopContext;

@ExtendWith(MockitoExtension.class)
class VerbValidatorTest {

  private static final String MARC_21 = "marc21";
  private static final String TEST_IDENTIFIER = "test_identifier";
  private static final String UNKNOWN_VERB = "unknown verb";
  private static final String VERB_NOT_IMPLEMENTED_ERROR_MESSAGE = "Bad verb. Verb \'%s\' is not implemented";
  private static final String DEFAULT_NAME_OF_NULL_VERB = "empty";
  private static final String MISSING_REQUIRED_PARAMETERS_ERROR_MESSAGE = "Missing required parameters: %s";
  private static final String RESUMPTION_TOKEN_TEST_VALUE = "resumptionTokenTestValue";
  private static final String EXCLUSIVE_PARAM_ERROR_MESSAGE = "Verb '%s', argument '%s' is exclusive, no others maybe specified with it.";
  private static final String TEST_VALUE = "test";

  private Map<String, String> requestParams = new HashMap<>();

  private VerbValidator validator = new VerbValidator();

  @Mock
  private Context context = new EventLoopContext(null, null, null, null, null, null, null);

  @After
  public void tearDown() {
    requestParams.clear();
  }

  @Test
  void shouldAddErrorWhenRequestedVerbIsNotImplemented() {
    List<OAIPMHerrorType> errors = validator.validate(UNKNOWN_VERB, context);
    assertTrue(isNotEmpty(errors));
    verifyContainsError(errors, BAD_VERB, format(VERB_NOT_IMPLEMENTED_ERROR_MESSAGE, UNKNOWN_VERB));
  }

  @Test
  void shouldAddErrorWhenRequestedVerbIsNull() {
    List<OAIPMHerrorType> errors = validator.validate(null, context);
    assertTrue(isNotEmpty(errors));
    verifyContainsError(errors, BAD_VERB, format(VERB_NOT_IMPLEMENTED_ERROR_MESSAGE, DEFAULT_NAME_OF_NULL_VERB));
  }

  @ParameterizedTest()
  @MethodSource("getVerbsWithRequiredParams")
  void shouldAddErrorWhenRequiredParametersIsMissed(Verb verb) {
    when(context.get(REQUEST_PARAMS)).thenReturn(requestParams);

    List<OAIPMHerrorType> errors = validator.validate(verb.toString(), context);
    String missedRequiredParams = getRequiredParamsAsString(verb);

    assertTrue(isNotEmpty(errors));
    verifyContainsError(errors, BAD_ARGUMENT, format(MISSING_REQUIRED_PARAMETERS_ERROR_MESSAGE, missedRequiredParams));
  }

  @ParameterizedTest
  @MethodSource("getVerbsWithExclusiveParams")
  void shouldAddErrorWhenParametersContainExclusiveAndAnotherOneWithIt(Verb verb) {
    when(context.get(REQUEST_PARAMS)).thenReturn(requestParams);
    requestParams.put(RESUMPTION_TOKEN_PARAM, RESUMPTION_TOKEN_TEST_VALUE);
    requestParams.put(FROM_PARAM, LocalDateTime.now()
      .format(ISO_UTC_DATE_ONLY));

    List<OAIPMHerrorType> errors = validator.validate(verb.toString(), context);

    assertTrue(isNotEmpty(errors));
    verifyContainsError(errors, BAD_ARGUMENT, format(EXCLUSIVE_PARAM_ERROR_MESSAGE, verb.name(), verb.getExclusiveParam()));
  }

  @ParameterizedTest
  @EnumSource(Verb.class)
  void shouldAddErrorWhenRequestParametersContainIllegalParameter(Verb verb) {
    when(context.get(REQUEST_PARAMS)).thenReturn(requestParams);
    setUpRequiredRequestParametersForVerb(verb);
    requestParams.put(getIllegalParameterForVerb(verb), TEST_VALUE);

    List<OAIPMHerrorType> errors = validator.validate(verb.toString(), context);

    assertTrue(isNotEmpty(errors));
    verifyContainsError(errors, BAD_ARGUMENT,
        format("Verb '%s', illegal argument: %s", verb.name(), getIllegalParameterForVerb(verb)));
  }

  @ParameterizedTest
  @EnumSource(Verb.class)
  void shouldReturnEmptyErrorListWhenParametersAreValid(Verb verb) {
    when(context.get(REQUEST_PARAMS)).thenReturn(requestParams);
    setUpRequiredRequestParametersForVerb(verb);

    List<OAIPMHerrorType> errors = validator.validate(verb.toString(), context);

    assertTrue(isEmpty(errors));
  }

  private void setUpRequiredRequestParametersForVerb(Verb verb) {
    verb.getRequiredParams()
      .forEach(param -> requestParams.put(param, setupParamTestValue(param)));
  }

  private String setupParamTestValue(String paramName) {
    switch (paramName) {
    case METADATA_PREFIX_PARAM:
      return MARC_21;
    case IDENTIFIER_PARAM:
      return TEST_IDENTIFIER;
    default:
      throw new IllegalArgumentException(format("Param with param name '%s' don't exist", paramName));
    }
  }

  private static Stream<Arguments> getVerbsWithRequiredParams() {
    Stream.Builder<Arguments> builder = Stream.builder();
    ImmutableList.of(Verb.GET_RECORD, Verb.LIST_IDENTIFIERS, Verb.LIST_RECORDS)
      .forEach(verb -> builder.add(Arguments.arguments(verb)));
    return builder.build();
  }

  private static Stream<Arguments> getVerbsWithExclusiveParams() {
    Stream.Builder<Arguments> builder = Stream.builder();
    ImmutableList.of(Verb.LIST_SETS, Verb.LIST_IDENTIFIERS, Verb.LIST_RECORDS)
      .forEach(verb -> builder.add(Arguments.arguments(verb)));
    return builder.build();
  }

  private String getIllegalParameterForVerb(Verb verb) {
    if (verb.equals(Verb.GET_RECORD) || verb.equals(Verb.LIST_METADATA_FORMATS)) {
      return SET_PARAM;
    } else {
      return IDENTIFIER_PARAM;
    }
  }

  private void verifyContainsError(List<OAIPMHerrorType> errors, OAIPMHerrorcodeType code, String errorMessage) {
    OAIPMHerrorType error = new OAIPMHerrorType().withCode(code)
      .withValue(errorMessage);
    assertTrue(errors.contains(error));
  }

  private String getRequiredParamsAsString(Verb verb) {
    return verb.getRequiredParams()
      .stream()
      .filter(p -> StringUtils.isEmpty(requestParams.get(p)))
      .collect(Collectors.joining(","));
  }
}
