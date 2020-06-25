package org.folio.oaipmh.helpers;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.BAD_DATESTAMP_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.CANNOT_DISSEMINATE_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.FROM_PARAM;
import static org.folio.oaipmh.Constants.ISO_UTC_DATE_ONLY;
import static org.folio.oaipmh.Constants.ISO_UTC_DATE_TIME;
import static org.folio.oaipmh.Constants.LIST_NO_REQUIRED_PARAM_ERROR;
import static org.folio.oaipmh.Constants.NEXT_RECORD_ID_PARAM;
import static org.folio.oaipmh.Constants.NO_RECORD_FOUND_ERROR;
import static org.folio.oaipmh.Constants.OFFSET_PARAM;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.REPOSITORY_TIME_GRANULARITY;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.TOTAL_RECORDS_PARAM;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.isDeletedRecordsEnabled;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.NO_RECORDS_MATCH;

import java.math.BigInteger;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseConverter;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.helpers.storage.StorageHelper;
import org.folio.rest.tools.client.HttpClientFactory;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.GranularityType;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.MetadataType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.SetType;
import org.openarchives.oai._2.StatusType;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Abstract helper implementation that provides some common methods.
 */
public abstract class AbstractHelper implements VerbHelper {

  private static final String DATE_ONLY_PATTERN = "^\\d{4}-\\d{2}-\\d{2}$";
  private static final Logger logger = LoggerFactory.getLogger(AbstractHelper.class);

  private static final String[] dateFormats = {
    org.apache.commons.lang.time.DateFormatUtils.ISO_DATE_FORMAT.getPattern(),
    "yyyy-MM-dd'T'HH:mm:ss'Z'"
  };

  private ResponseHelper responseHelper = ResponseHelper.getInstance();

  /**
   * Holds instance to handle items returned
   */
  protected StorageHelper storageHelper = StorageHelper.getInstance();

  /**
   * The method is intended to be used to validate 'ListIdentifiers' and 'ListRecords' requests
   *
   * @param request the {link Request} with parameters to be validated
   * @return {@link List} of the {@link OAIPMHerrorType} if there is any validation error or empty list
   */
  protected List<OAIPMHerrorType> validateListRequest(Request request) {
    List<OAIPMHerrorType> errors = new ArrayList<>();

    if (request.getMetadataPrefix() != null) {
      if (!MetadataPrefix.getAllMetadataFormats().contains(request.getMetadataPrefix())) {
        errors.add(new OAIPMHerrorType().withCode(CANNOT_DISSEMINATE_FORMAT).withValue(CANNOT_DISSEMINATE_FORMAT_ERROR));
      }
    } else {
      errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT)
        .withValue(LIST_NO_REQUIRED_PARAM_ERROR));
    }

    if (request.getSet() != null && !getSupportedSetSpecs().contains(request.getSet())) {
      errors.add(createNoRecordsFoundError());
    }

    if (isNotEmpty(request.getFrom()) || isNotEmpty(request.getUntil())) {
      validateDateRange(request, errors);
    }

    return errors;
  }

  private void validateDateRange(Request request, List<OAIPMHerrorType> errors) {
    Pair<GranularityType, LocalDateTime> from = null;
    Pair<GranularityType, LocalDateTime> until = null;
    // Get repository supported granularity
    boolean isDateOnly = isDateOnlyGranularity(request);
    if (request.getFrom() != null) {
      ImmutablePair<String, String> date = new ImmutablePair<>(FROM_PARAM, request.getFrom());
      from = isDateOnly ? parseDate(date, errors) : parseDateTime(date, errors);
      if (from == null) {
        // In case the 'from' date is invalid, it cannot be sent in OAI-PMH/request@from because it contradicts schema definition
        request.getOaiRequest().setFrom(null);
      }
    }
    if (request.getUntil() != null) {
      ImmutablePair<String, String> date = new ImmutablePair<>(UNTIL_PARAM, request.getUntil());
      until = isDateOnly ? parseDate(date, errors) : parseDateTime(date, errors);
      if (until == null) {
        // In case the 'until' date is invalid, it cannot be sent in OAI-PMH/request@until because it contradicts schema definition
        request.getOaiRequest().setUntil(null);
      }
    }
    if (from != null && until != null) {
      // Both arguments must have the same granularity.
      if (from.getLeft() != until.getLeft()) {
        errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT)
          .withValue("Invalid date range: 'from' must have the same granularity as 'until'."));
      } else if (from.getRight().isAfter(until.getRight())) {
        // The from argument must be less than or equal to the until argument.
        errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT)
          .withValue("Invalid date range: 'from' must be less than or equal to 'until'."));
      }
    }
  }

  private boolean isDateOnlyGranularity(Request request) {
    String granularity = RepositoryConfigurationUtil.getProperty
      (request.getOkapiHeaders().get(OKAPI_TENANT), REPOSITORY_TIME_GRANULARITY);
    return GranularityType.fromValue(granularity) == GranularityType.YYYY_MM_DD;
  }

  /**
   * Validates if the date time is in {@linkplain org.folio.oaipmh.Constants#ISO_UTC_DATE_TIME ISO_UTC_DATE_TIME} format.
   * If the date is valid, {@link LocalDateTime} is returned. Otherwise {@link #parseDate(Pair, List)} is called because repository
   * must support {@linkplain GranularityType#YYYY_MM_DD YYYY_MM_DD} granularity and date might be in such format
   *
   * @param date   {@link Pair} where key (left element) is parameter name (i.e. from or until), value (right element) is date time
   * @param errors list of errors to be updated if format is wrong
   * @return {@link LocalDateTime} if date format is valid, {@literal null} otherwise.
   */
  private Pair<GranularityType, LocalDateTime> parseDateTime(Pair<String, String> date, List<OAIPMHerrorType> errors) {
    try {
      LocalDateTime dateTime = LocalDateTime.parse(date.getValue(), ISO_UTC_DATE_TIME);
      return new ImmutablePair<>(GranularityType.YYYY_MM_DD_THH_MM_SS_Z, dateTime);
    } catch (DateTimeParseException e) {
      // The repository must support YYYY-MM-DD granularity so try to parse date only
      return parseDate(date, errors);
    }
  }

  /**
   * Validates if the date is in {@link DateTimeFormatter#ISO_LOCAL_DATE} format.
   * If the date is valid, it is returned as {@link LocalDateTime} at time of midnight.
   * Otherwise {@literal null} is returned and validation error is added to errors list
   *
   * @param date   {@link Pair} where key (left element) is parameter name (i.e. from or until), value (right element) is date
   * @param errors list of errors to be updated if format is wrong
   * @return {@link LocalDateTime} if date format is valid, {@literal null} otherwise.
   */
  private Pair<GranularityType, LocalDateTime> parseDate(Pair<String, String> date, List<OAIPMHerrorType> errors) {
    try {
      LocalDateTime dateTime = LocalDate.parse(date.getValue()).atStartOfDay();
      return new ImmutablePair<>(GranularityType.YYYY_MM_DD, dateTime);
    } catch (DateTimeParseException e) {
      errors.add(new OAIPMHerrorType()
        .withCode(BAD_ARGUMENT)
        .withValue(String.format(BAD_DATESTAMP_FORMAT_ERROR, date.getKey(), date.getValue())));
      return null;
    }
  }

  protected Date convertStringToDate(String dateTimeString) {
    try {
      if (dateTimeString.isEmpty()) {
        return null;
      }
      return DateUtils.parseDate(dateTimeString, dateFormats);
    } catch (DateTimeParseException | ParseException e) {
      logger.error(e);
      return null;

    }
  }

  protected boolean validateIdentifier(Request request) {
    return StringUtils.startsWith(request.getIdentifier(), request.getIdentifierPrefix());
  }

  protected OAIPMHerrorType createNoRecordsFoundError() {
    return new OAIPMHerrorType().withCode(NO_RECORDS_MATCH).withValue(NO_RECORD_FOUND_ERROR);
  }

  protected List<SetType> getSupportedSetTypes() {
    List<SetType> sets = new ArrayList<>();
    sets.add(new SetType()
      .withSetSpec("all")
      .withSetName("All records"));
    return sets;
  }

  /**
   * Creates {@link HeaderType} and populates Identifier, Datestamp and Set
   *
   * @param identifierPrefix oai-identifier prefix
   * @param instance         the instance item returned by storage service
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
   *
   * @param instance the instance item returned by storage service
   * @return {@link Instant} based on updated or created date
   */
  private Instant getInstanceDate(JsonObject instance) {
    return storageHelper.getLastModifiedDate(instance);
  }

  /**
   * Builds oai-identifier
   *
   * @param identifierPrefix oai-identifier prefix
   * @param instance         the instance item returned by storage service
   * @return oai-identifier
   */
  private String getIdentifier(String identifierPrefix, JsonObject instance) {
    return getIdentifier(identifierPrefix, storageHelper.getIdentifierId(instance));
  }

  /**
   * Builds oai-identifier
   *
   * @param identifierPrefix oai-identifier prefix
   * @param storageId        id of the instance returned by storage service
   * @return oai-identifier
   */
  protected String getIdentifier(String identifierPrefix, String storageId) {
    return identifierPrefix + storageId;
  }

  /**
   * Builds resumptionToken that is used to resume request sequence
   * in case the whole result set is partitioned.
   *
   * @param request      the initial request
   * @param instances    the array of instances returned from Instance Storage
   * @param totalRecords the total number of records in the whole result set
   * @return resumptionToken value if partitioning is used and not all instances are processed yet,
   * empty string if partitioning is used and all instances are processed already,
   * null if the result set is not partitioned.
   */
  protected ResumptionTokenType buildResumptionToken(Request request, JsonArray instances, Integer totalRecords) {
    int newOffset = request.getOffset() + Integer.parseInt(RepositoryConfigurationUtil.getProperty
      (request.getOkapiHeaders().get(OKAPI_TENANT), REPOSITORY_MAX_RECORDS_PER_RESPONSE));
    String resumptionToken = request.isRestored() ? EMPTY : null;
    if (newOffset < totalRecords) {
      Map<String, String> extraParams = new HashMap<>();
      extraParams.put(TOTAL_RECORDS_PARAM, String.valueOf(totalRecords));
      extraParams.put(OFFSET_PARAM, String.valueOf(newOffset));
      String nextRecordId;
      if (isDeletedRecordsEnabled(request)) {
        nextRecordId = storageHelper.getId(getLastInstance(instances));
      } else {
        nextRecordId = storageHelper.getRecordId(getLastInstance(instances));
      }
      extraParams.put(NEXT_RECORD_ID_PARAM, nextRecordId);
      if (request.getUntil() == null) {
        extraParams.put(UNTIL_PARAM, getUntilDate(request, request.getFrom()));
      }

      resumptionToken = request.toResumptionToken(extraParams);
    }

    if (resumptionToken != null) {
      return new ResumptionTokenType()
        .withValue(resumptionToken)
        .withCompleteListSize(BigInteger.valueOf(totalRecords))
        .withCursor(request.getOffset() == 0 ? BigInteger.ZERO : BigInteger.valueOf(request.getOffset()));
    }

    return null;
  }

  private JsonObject getLastInstance(JsonArray instances) {
    return (JsonObject) instances.remove(instances.size() - 1);
  }

  /**
   * Returns string representation of built LocalDateTime object. If repository.timeGranularity == yyyy-MM-dd format then only this
   * format has to be used for building LocalDateTime. If repository.timeGranularity == yyyy-MM-ddTHH:mm:ssZ then since such format
   * allows both formats for 'from' parameter (date+time or date only) then 'until' value should be built using the same format as
   * 'from' has with purpose to keep the matching the formats of both parameters.
   *
   * @param request - OIA-PMH request
   * @param from    - from parameter
   * @return string representation of built LocalDateTime object
   */
  protected String getUntilDate(Request request, String from) {
    boolean isDateOnly = isDateOnlyGranularity(request);
    if (isDateOnly) {
      return LocalDateTime.now().format(ISO_UTC_DATE_ONLY);
    } else {
      return isNotEmpty(from) && from.matches(DATE_ONLY_PATTERN) ? LocalDateTime.now().format(ISO_UTC_DATE_ONLY)
        : LocalDateTime.now().format(ISO_UTC_DATE_TIME);
    }
  }

  /**
   * Filter records from source-record-storage by the fields "deleted = true" or MARC Leader 05 is "d", "s" or "x"
   * For old behavior (repository.deletedRecords is "no") fields are checked only according to the previously described criteria.
   * In case when repository.deletedRecords is "persistent" or "transient" checked suppressedRecordsProcessing config setting
   *
   * @param request  OIA-PMH request
   * @param instance record in JsonObject format
   * @return true when a record should be present in oai-pmh response
   */
  protected boolean filterInstance(Request request, JsonObject instance) {
    if (!isDeletedRecordsEnabled(request)) {
      return !storageHelper.isRecordMarkAsDeleted(instance);
    } else {
      Map<String, String> okapiHeaders = request.getOkapiHeaders();
      boolean shouldProcessSuppressedRecords = getBooleanProperty(okapiHeaders, REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
      return shouldProcessSuppressedRecords || !storageHelper.getSuppressedFromDiscovery(instance)
        || storageHelper.isRecordMarkAsDeleted(instance);
    }
  }

  protected HeaderType addHeader(String identifierPrefix, Request request, JsonObject instance) {
    HeaderType header = populateHeader(identifierPrefix, instance);
    if (isDeletedRecordsEnabled(request) && storageHelper.isRecordMarkAsDeleted(instance)) {
      header.setStatus(StatusType.DELETED);
    }
    return header;
  }

  /**
   * Checks if request sequences can be resumed without losing records in case of partitioning the whole result set.
   * <br/>
   * The following state is an indicator that flow cannot be safely resumed:
   * <li>No instances are returned</li>
   * <li>Current total number of records is less than the previous one and the first
   * record id does not match one stored in the resumptionToken</li>
   * <br/>
   * See <a href="https://issues.folio.org/browse/MODOAIPMH-10">MODOAIPMH-10</a> for more details.
   *
   * @param request
   * @param totalRecords
   * @param instances
   * @return
   */
  protected boolean canResumeRequestSequence(Request request, Integer totalRecords, JsonArray instances) {
    Integer prevTotalRecords = request.getTotalRecords();
    boolean isDeletedRecords = isDeletedRecordsEnabled(request);
    int firstPosition = 0;
    return instances != null && instances.size() > 0
        && (totalRecords >= prevTotalRecords || StringUtils.equals(request.getNextRecordId(),
            isDeletedRecords ? storageHelper.getId(instances.getJsonObject(firstPosition))
                : storageHelper.getRecordId(instances.getJsonObject(firstPosition))));
  }

  private List<String> getSupportedSetSpecs() {
    return getSupportedSetTypes().stream()
      .map(SetType::getSetSpec)
      .collect(Collectors.toList());
  }

  protected ResponseHelper getResponseHelper() {
    return responseHelper;
  }

  protected Future<Response> buildResponseWithErrors(Request request, Promise<Response> promise, List<OAIPMHerrorType> errors) {
    OAIPMH oai;
    if (request.isRestored()) {
      oai = responseHelper.buildOaipmhResponseWithErrors(request, BAD_RESUMPTION_TOKEN,
        RESUMPTION_TOKEN_FORMAT_ERROR);
    } else {
      oai = responseHelper.buildOaipmhResponseWithErrors(request, errors);
    }
    promise.complete(responseHelper.buildFailureResponse(oai, request));
    return promise.future();
  }

  protected MetadataType buildOaiMetadata(Request request, String content) {
    MetadataType metadata = new MetadataType();
    MetadataPrefix metadataPrefix = MetadataPrefix.fromName(request.getMetadataPrefix());
    byte[] byteSource = metadataPrefix.convert(content);
    Object record = ResponseConverter.getInstance().bytesToObject(byteSource);
    metadata.setAny(record);
    return metadata;
  }

  protected boolean hasRecordsWithoutMetadata(Map<String, RecordType> records) {
    return records.values()
      .stream()
      .map(RecordType::getMetadata)
      .anyMatch(Objects::isNull);
  }

}
