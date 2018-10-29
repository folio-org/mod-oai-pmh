package org.folio.oaipmh.helpers;

import io.vertx.core.json.JsonObject;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.tools.client.HttpClientFactory;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RequestType;
import org.openarchives.oai._2.SetType;
import org.openarchives.oai._2_0.oai_identifier.OaiIdentifier;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.NO_RECORDS_MATCH;

/**
 * Abstract helper implementation that provides some common methods.
 */
public abstract class AbstractHelper implements VerbHelper {

  public static final String REPOSITORY_BASE_URL = "repository.baseURL";
  public static final String CANNOT_DISSEMINATE_FORMAT_ERROR = "The value '%s' of the metadataPrefix argument is not supported by the repository";
  public static final String RESUMPTION_TOKEN_FORMAT_ERROR = "The value '%s' of the resumptionToken argument is invalid";
  public static final String LIST_NO_REQUIRED_PARAM_ERROR = "The request is missing required arguments. There is no metadataPrefix nor resumptionToken";
  public static final String LIST_ILLEGAL_ARGUMENTS_ERROR = "The request includes resumptionToken and other argument(s)";
  public static final String NO_RECORD_FOUND_ERROR = "There is no any record found matching search criteria";

  /** Strict ISO Date and Time with UTC offset. */
  private static final DateTimeFormatter ISO_UTC_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

  /**
   * The value should be populated only once at start up. So considered to be constant.
   * @see #createRepositoryIdentifierPrefix()
   */
  private static String OAI_IDENTIFIER_PREFIX;

  /**
   * Holds instance to handle items returned
   */
  protected static InstancesStorageHelper storageHelper;

  /**
   * Initializes data required for OAI-PMH responses
   */
  public static void init() {
    createRepositoryIdentifierPrefix();
    storageHelper = InstancesStorageHelper.getStorageHelper();
  }

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
      from = parseDate(request.getFrom(), errors);
      if (from == null) {
        // In case the 'from' date is invalid, we cannot send it in OAI-PMH/request@from because it contradicts schema definition
        request.getOaiRequest().setFrom(null);
      }
    }
    if (request.getUntil() != null) {
      until = parseDate(request.getUntil(), errors);
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

  private LocalDateTime parseDate(String date, List<OAIPMHerrorType> errors) {
    try {
      return LocalDateTime.parse(date, ISO_UTC_DATE_TIME);
    } catch (DateTimeParseException e) {
      errors.add(new OAIPMHerrorType()
        .withCode(BAD_ARGUMENT)
        .withValue(String.format("Bad datestamp format for '%s' argument.", date)));
      return null;
    }
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
   * Return the repository name.
   * For now, it is based on System property, but later it might be pulled from mod-configuration.
   *
   * @return repository name
   */
  protected HttpClientInterface getOkapiClient(Map<String, String> okapiHeaders) {
    String tenantId = TenantTool.tenantId(okapiHeaders);
    String okapiURL = okapiHeaders.get(XOkapiHeaders.URL);
    return HttpClientFactory.getHttpClient(okapiURL, tenantId, true);
  }

  /**
   * Creates {@link HeaderType} and populates Identifier, Datestamp and Set
   *
   * @param tenantId tenant id
   * @param instance the instance item returned by storage service
   * @return populated {@link HeaderType}
   */
  protected HeaderType populateHeader(String tenantId, JsonObject instance) {
    return new HeaderType()
      .withIdentifier(getIdentifier(tenantId, instance))
      .withDatestamp(getInstanceDate(instance))
      .withSetSpecs("all");
  }

  /**
   * Creates oai-identifier prefix based on {@link OaiIdentifier}. The format is {@literal oai:<repositoryIdentifier>}.
   * The repositoryIdentifier is based on repository base URL (host part).
   * @see <a href="http://www.openarchives.org/OAI/2.0/guidelines-oai-identifier.htm">OAI Identifier Format</a>
   */
  private static void createRepositoryIdentifierPrefix() {
    String baseURL = getBaseURL();
    try {
      URL url = new URL(baseURL);
      OaiIdentifier oaiIdentifier = new OaiIdentifier();
      oaiIdentifier.setRepositoryIdentifier(url.getHost());

      OAI_IDENTIFIER_PREFIX = oaiIdentifier.getScheme() + oaiIdentifier.getDelimiter()
        + oaiIdentifier.getRepositoryIdentifier() + oaiIdentifier.getDelimiter();
    } catch (MalformedURLException e) {
      throw new IllegalStateException(e);
    }
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
   * @param tenantId tenant id
   * @param instance the instance item returned by storage service
   * @return oai-identifier
   */
  private String getIdentifier(String tenantId, JsonObject instance) {
    return  OAI_IDENTIFIER_PREFIX + tenantId + "/" + storageHelper.getItemId(instance);
  }

  private List<String> getSupportedSetSpecs() {
    return getSupportedSetTypes().stream()
                                 .map(SetType::getSetSpec)
                                 .collect(Collectors.toList());
  }
}
