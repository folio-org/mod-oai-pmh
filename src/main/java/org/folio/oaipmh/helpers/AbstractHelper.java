package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.tools.client.HttpClientFactory;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.RequestType;
import org.openarchives.oai._2.SetType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.BAD_DATESTAMP_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.CANNOT_DISSEMINATE_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.FROM_PARAM;
import static org.folio.oaipmh.Constants.IDENTIFIER_PREFIX;
import static org.folio.oaipmh.Constants.LIST_ILLEGAL_ARGUMENTS_ERROR;
import static org.folio.oaipmh.Constants.LIST_NO_REQUIRED_PARAM_ERROR;
import static org.folio.oaipmh.Constants.NO_RECORD_FOUND_ERROR;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.NO_RECORDS_MATCH;

/**
 * Abstract helper implementation that provides some common methods.
 */
public abstract class AbstractHelper implements VerbHelper {

  /** Strict ISO Date and Time with UTC offset. */
  private static final DateTimeFormatter ISO_UTC_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

  /**
   * Holds instance to handle items returned
   */
  protected static InstancesStorageHelper storageHelper = InstancesStorageHelper.getStorageHelper();

  /**
   * Creates basic {@link OAIPMH} with ResponseDate and Request details
   * @param request {@link Request}
   * @return basic {@link OAIPMH}
   */
  protected OAIPMH buildBaseResponse(RequestType request) {
    return new OAIPMH()
      // According to spec the nanoseconds should not be used so truncate to seconds
      .withResponseDate(Instant.now().truncatedTo(ChronoUnit.SECONDS))
      .withRequest(request.withValue(getBaseURL()));
  }

  /**
   * The method is intended to be used to validate 'ListIdentifiers' and 'ListRecords' requests
   * @param request the {link Request} with parameters to be validated
   * @return {@link List} of the {@link OAIPMHerrorType} if there is any validation error or empty list
   */
  protected List<OAIPMHerrorType> validateListRequest(Request request) {
    List<OAIPMHerrorType> errors = new ArrayList<>();

    // The 'resumptionToken' is an exclusive argument. We need to check that if it is, there is nothing else
    boolean hasResumptionToken = request.getResumptionToken() != null;
    boolean hasOtherParam = false;

    if (hasResumptionToken) {
      // At the moment the 'resumptionToken' is not supported so any format is considered invalid so far
      errors.add(new OAIPMHerrorType().withCode(BAD_RESUMPTION_TOKEN)
                                      .withValue(String.format(RESUMPTION_TOKEN_FORMAT_ERROR, request.getResumptionToken())));
    }

    // The 'metadataPrefix' parameter is required only if there is no 'resumptionToken'
    if (request.getMetadataPrefix() != null) {
      hasOtherParam = true;
      if (!MetadataPrefix.getAllMetadataFormats().contains(request.getMetadataPrefix())) {
        errors.add(new OAIPMHerrorType().withCode(CANNOT_DISSEMINATE_FORMAT)
          .withValue(String.format(CANNOT_DISSEMINATE_FORMAT_ERROR, request.getMetadataPrefix())));
      }
    } else if (!hasResumptionToken) {
      errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT).withValue(LIST_NO_REQUIRED_PARAM_ERROR));
    }

    if (request.getSet() != null) {
      hasOtherParam = true;
      if (!getSupportedSetSpecs().contains(request.getSet())) {
        errors.add(createNoRecordsFoundError());
      }
    }

    if (isNotEmpty(request.getFrom()) || isNotEmpty(request.getUntil())) {
      hasOtherParam = true;
      validateDateRange(request, errors);
    }


    if (hasResumptionToken && hasOtherParam) {
      errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT).withValue(LIST_ILLEGAL_ARGUMENTS_ERROR));
    }
    return errors;
  }

  private void validateDateRange(Request request, List<OAIPMHerrorType> errors) {
    LocalDateTime from = null;
    LocalDateTime until = null;
    if (request.getFrom() != null) {
      from = parseDate(new ImmutablePair<>(FROM_PARAM, request.getFrom()), errors);
      if (from == null) {
        // In case the 'from' date is invalid, we cannot send it in OAI-PMH/request@from because it contradicts schema definition
        request.getOaiRequest().setFrom(null);
      }
    }
    if (request.getUntil() != null) {
      until = parseDate(new ImmutablePair<>(UNTIL_PARAM, request.getUntil()), errors);
      if (until == null) {
        // In case the 'until' date is invalid, we cannot send it in OAI-PMH/request@until because it contradicts schema definition
        request.getOaiRequest().setUntil(null);
      }
    }
    if (from != null && until != null && from.isAfter(until)) {
      errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT)
        .withValue("Invalid date range: 'from' must be less than or equal to 'until'."));
    }
  }

  /**
   * Validates if the date time format is in {@link #ISO_UTC_DATE_TIME} format. If the date is valid, it is returned as {@link LocalDateTime}.
   * Otherwise {@literal null} is returned and validation error is added to errors list
   * @param date {@link Pair} where key (left element) is parameter name (i.e. from or until), value (right element) is date time
   * @param errors list of errors to be updated if format is wrong
   * @return {@link LocalDateTime} if date format is valid, {@literal null} otherwise.
   */
  private LocalDateTime parseDate(Pair<String, String> date, List<OAIPMHerrorType> errors) {
    try {
      return LocalDateTime.parse(date.getValue(), ISO_UTC_DATE_TIME);
    } catch (DateTimeParseException e) {
      errors.add(new OAIPMHerrorType()
        .withCode(BAD_ARGUMENT)
        .withValue(String.format(BAD_DATESTAMP_FORMAT_ERROR, date.getKey(), date.getValue())));
      return null;
    }
  }

  protected static boolean validateIdentifier(Request request) {
    return StringUtils.startsWith(request.getIdentifier(), request.getIdentifierPrefix());
  }

  protected OAIPMHerrorType createNoRecordsFoundError() {
    return new OAIPMHerrorType().withCode(NO_RECORDS_MATCH).withValue(NO_RECORD_FOUND_ERROR);
  }

  /**
   * Return the repository base URL.
   * For now, it is based on System property, but later it might be pulled from mod-configuration.
   *
   * @return repository base URL
   */
  protected static String getBaseURL() {
    String baseUrl = System.getProperty(REPOSITORY_BASE_URL);
    if (baseUrl == null) {
      throw new IllegalStateException("The required repository config 'repository.baseURL' is missing");
    }
    return baseUrl;
  }

  protected List<SetType> getSupportedSetTypes() {
    List<SetType> sets = new ArrayList<>();
    sets.add(new SetType()
      .withSetSpec("all")
      .withSetName("All records"));
    return sets;
  }

  /**
   * Creates Okapi client getting Okapi URL from headers. The connection will be closed automatically if idle
   */
  protected HttpClientInterface getOkapiClient(Map<String, String> okapiHeaders) {
    return getOkapiClient(okapiHeaders, true);
  }

  /**
   * Creates Okapi client getting Okapi URL from headers.
   */
  protected HttpClientInterface getOkapiClient(Map<String, String> okapiHeaders, boolean autoCloseConnections) {
    String tenantId = TenantTool.tenantId(okapiHeaders);
    String okapiURL = okapiHeaders.get(XOkapiHeaders.URL);
    return HttpClientFactory.getHttpClient(okapiURL, tenantId, autoCloseConnections);
  }

  /**
   * Creates {@link HeaderType} and populates Identifier, Datestamp and Set
   *
   * @param identifierPrefix oai-identifier prefix
   * @param instance the instance item returned by storage service
   * @return populated {@link HeaderType}
   */
  protected HeaderType populateHeader(String identifierPrefix, JsonObject instance) {
    return createHeader(instance)
      .withIdentifier(getIdentifier(identifierPrefix, instance));
  }

  /**
   * Creates {@link HeaderType} and Datestamp and Set
   *
   * @param instance the instance item returned by storage service
   * @return populated {@link HeaderType}
   */
  protected HeaderType createHeader(JsonObject instance) {
    return new HeaderType()
      .withDatestamp(getInstanceDate(instance))
      .withSetSpecs("all");
  }

  /**
   * Returns last modified or created date
   * @param instance the instance item returned by storage service
   * @return {@link Instant} based on updated or created date
   */
  private Instant getInstanceDate(JsonObject instance) {
    return storageHelper.getLastModifiedDate(instance);
  }

  /**
   * Builds oai-identifier
   * @param identifierPrefix oai-identifier prefix
   * @param instance the instance item returned by storage service
   * @return oai-identifier
   */
  private String getIdentifier(String identifierPrefix, JsonObject instance) {
    return identifierPrefix + storageHelper.getItemId(instance);
  }

  /**
   * The error codes required to define the http code to be returned in the http response
   * @param oai OAIPMH response with errors
   * @return set of error codes
   */
  protected Set<OAIPMHerrorcodeType> getErrorCodes(OAIPMH oai) {
    // According to oai-pmh.raml the service will return different http codes depending on the error
    return oai.getErrors()
              .stream()
              .map(OAIPMHerrorType::getCode)
              .collect(Collectors.toSet());
  }

  private List<String> getSupportedSetSpecs() {
    return getSupportedSetTypes().stream()
                                 .map(SetType::getSetSpec)
                                 .collect(Collectors.toList());
  }
}
