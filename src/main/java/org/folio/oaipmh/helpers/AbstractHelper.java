package org.folio.oaipmh.helpers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseConverter;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.helpers.storage.StorageHelper;
import org.folio.oaipmh.service.ErrorsService;
import org.openarchives.oai._2.GranularityType;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.MetadataType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.SetType;
import org.openarchives.oai._2.StatusType;
import org.openarchives.oai._2.VerbType;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.oaipmh.Constants.BAD_DATESTAMP_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.CANNOT_DISSEMINATE_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.EXPIRATION_DATE_RESUMPTION_TOKEN_PARAM;
import static org.folio.oaipmh.Constants.FROM_PARAM;
import static org.folio.oaipmh.Constants.INVENTORY;
import static org.folio.oaipmh.Constants.ISO_DATE_TIME_PATTERN;
import static org.folio.oaipmh.Constants.ISO_UTC_DATE_ONLY;
import static org.folio.oaipmh.Constants.ISO_UTC_DATE_TIME;
import static org.folio.oaipmh.Constants.LIST_NO_REQUIRED_PARAM_ERROR;
import static org.folio.oaipmh.Constants.NEXT_RECORD_ID_PARAM;
import static org.folio.oaipmh.Constants.NO_RECORD_FOUND_ERROR;
import static org.folio.oaipmh.Constants.INVALID_CHARACTER_IN_THE_RECORD;
import static org.folio.oaipmh.Constants.OFFSET_PARAM;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_RECORDS_SOURCE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.REPOSITORY_TIME_GRANULARITY;
import static org.folio.oaipmh.Constants.REQUEST_CURSOR_PARAM;
import static org.folio.oaipmh.Constants.REQUEST_FROM_INVENTORY_PARAM;
import static org.folio.oaipmh.Constants.REQUEST_INVENTORY_OFFSET_SHIFT_PARAM;
import static org.folio.oaipmh.Constants.REQUEST_INVENTORY_TOTAL_RECORDS_PARAM;
import static org.folio.oaipmh.Constants.REQUEST_OLD_SRS_OFFSET_PARAM;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_TIMEOUT;
import static org.folio.oaipmh.Constants.SRS;
import static org.folio.oaipmh.Constants.TOTAL_RECORDS_PARAM;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.isDeletedRecordsEnabled;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.ID_DOES_NOT_EXIST;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.NO_RECORDS_MATCH;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.INVALID_RECORD_CONTENT;

/**
 * Abstract helper implementation that provides some common methods.
 */
public abstract class AbstractHelper implements VerbHelper {

  private static final Logger logger = LogManager.getLogger(AbstractHelper.class);

  private static final String DATE_ONLY_PATTERN = "^\\d{4}-\\d{2}-\\d{2}$";
  protected final DateFormat dateFormat = new SimpleDateFormat(ISO_DATE_TIME_PATTERN);

  private static final String[] dateFormats = {
    DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.getPattern(),
    ISO_DATE_TIME_PATTERN
  };

  private static ResponseHelper responseHelper = ResponseHelper.getInstance();

  /**
   * Holds instance to handle items returned
   */
  protected StorageHelper storageHelper = StorageHelper.getInstance();

  protected ErrorsService errorsService;

  public Response conversionIntoJaxbObjectIssueResponse(OAIPMH oaipmh, Request request) {
    oaipmh.withErrors(createInvalidJsonContentError());
    return getResponseHelper().buildFailureResponse(oaipmh, request);
  }
  public Response buildNoRecordsFoundOaiResponse(OAIPMH oaipmh, Request request) {
    oaipmh.withErrors(createNoRecordsFoundError());
    return getResponseHelper().buildFailureResponse(oaipmh, request);
  }

  public static Response buildNoRecordsFoundOaiResponse(OAIPMH oaipmh, Request request, String errorMessage) {
    if (StringUtils.isNotEmpty(errorMessage)) {
      var verb = request.getVerb();
      if (verb == VerbType.GET_RECORD) {
        oaipmh.withErrors(new OAIPMHerrorType().withCode(ID_DOES_NOT_EXIST).withValue(errorMessage));
      } else {
        oaipmh.withErrors(new OAIPMHerrorType().withCode(NO_RECORDS_MATCH).withValue(errorMessage));
      }
    } else {
      oaipmh.withErrors(createNoRecordsFoundError());
    }
    return getResponseHelper().buildFailureResponse(oaipmh, request);
  }

  protected static OAIPMHerrorType createNoRecordsFoundError() {
    return new OAIPMHerrorType().withCode(NO_RECORDS_MATCH).withValue(NO_RECORD_FOUND_ERROR);
  }
  protected static OAIPMHerrorType createInvalidJsonContentError() {
    return new OAIPMHerrorType().withCode(INVALID_RECORD_CONTENT).withValue(INVALID_CHARACTER_IN_THE_RECORD);
  }

  public static ResponseHelper getResponseHelper() {
    return responseHelper;
  }

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
      (request.getRequestId(), REPOSITORY_TIME_GRANULARITY);
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
    if (date.getValue().isEmpty()) {
      return null;
    }
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
    if (date.getValue().isEmpty()) {
      return null;
    }
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

  /**
   * Parse a date from string and compensate one date or one second because in SRS the dates are non-inclusive.
   *
   * @param dateTimeString                 - date/time supplied
   * @param shouldCompensateUntilDate      = if the date is used as until parameter
   * @param shouldEqualizeTimeBetweenZones whether the time should be updated by diff between current time zone and UTC
   * @return date that will be used to query SRS
   */
  protected Date convertStringToDate(String dateTimeString, boolean shouldCompensateUntilDate, boolean shouldEqualizeTimeBetweenZones) {
    try {
      if (StringUtils.isEmpty(dateTimeString)) {
        return null;
      }
      Date date = DateUtils.parseDate(dateTimeString, dateFormats);
      if (shouldCompensateUntilDate) {
        if (dateTimeString.matches(DATE_ONLY_PATTERN)) {
          date = DateUtils.addDays(date, 1);
        } else {
          date = DateUtils.addSeconds(date, 1);
        }
      }
      if (shouldEqualizeTimeBetweenZones) {
        return addTimeDiffBetweenCurrentTimeZoneAndUTC(date);
      }
      return date;
    } catch (DateTimeParseException | ParseException e) {
      logger.error(e);
      return null;
    }
  }

  /**
   * Adds difference between the current time zone and UTC to 'date' param. Is used when 'date' is going to be used for SRC client
   * search criteria by the reason that SRC equates the date to UTC date by subtracting the diff between current time zone and UTC
   * which leads to invalid time borders requesting.
   *
   * @param date - date to be updated
   * @return updated date
   */
  private Date addTimeDiffBetweenCurrentTimeZoneAndUTC(Date date) {
    int secondsDiff = ZoneId.systemDefault()
      .getRules()
      .getOffset(date.toInstant())
      .getTotalSeconds();
    Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
    calendar.setTime(date);
    calendar.add(Calendar.SECOND, secondsDiff);
    return calendar.getTime();
  }

  protected boolean validateIdentifier(Request request) {
    return StringUtils.startsWith(request.getIdentifier(), request.getIdentifierPrefix());
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
   * @param request          oai-pmh request
   * @return populated {@link HeaderType}
   */
  protected HeaderType populateHeader(String identifierPrefix, JsonObject instance, Request request) {
    return createHeader(instance, request)
      .withIdentifier(getIdentifier(identifierPrefix, instance));
  }

  /**
   * Creates {@link HeaderType} and Datestamp and Set
   *
   * @param instance the instance item returned by storage service
   * @param request  oai-pmh request
   * @return populated {@link HeaderType}
   */
  protected HeaderType createHeader(JsonObject instance, Request request) {
    HeaderType headerType = new HeaderType().withSetSpecs("all");
    Instant instant = getInstanceDate(instance);
    String date;
    if (isDateOnlyGranularity(request)) {
      instant = instant.truncatedTo(ChronoUnit.DAYS);
      LocalDate time = LocalDate.ofInstant(instant, ZoneId.of("UTC"));
      date = ISO_UTC_DATE_ONLY.format(time);
    } else {
      LocalDateTime time = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
      date = ISO_UTC_DATE_TIME.format(time);
    }
    return headerType.withDatestamp(date);
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
    int inventoryOffsetShift = request.getInventoryOffsetShift();
    int maxRecordsPerResponse = Integer.parseInt(RepositoryConfigurationUtil.getProperty
      (request.getRequestId(), REPOSITORY_MAX_RECORDS_PER_RESPONSE));
    int newOffset = request.getOffset() + maxRecordsPerResponse + inventoryOffsetShift;
    int newCursor = request.getCursor() + instances.size() - 1;
    request.setInventoryOffsetShift(0);
    String resumptionToken = request.isRestored() ? EMPTY : null;
    Map<String, String> extraParams = new HashMap<>();
    if (newOffset < (request.isFromInventory() ? request.getInventoryTotalRecords() : totalRecords)) {
      extraParams.put(TOTAL_RECORDS_PARAM, String.valueOf(totalRecords));
      extraParams.put(OFFSET_PARAM, String.valueOf(newOffset));
      extraParams.put(EXPIRATION_DATE_RESUMPTION_TOKEN_PARAM, String.valueOf(Instant.now().with(ChronoField.NANO_OF_SECOND, 0).plusSeconds(RESUMPTION_TOKEN_TIMEOUT)));
      extraParams.put(REQUEST_FROM_INVENTORY_PARAM, String.valueOf(request.isFromInventory()));
      extraParams.put(REQUEST_INVENTORY_TOTAL_RECORDS_PARAM, String.valueOf(request.getInventoryTotalRecords()));
      extraParams.put(REQUEST_INVENTORY_OFFSET_SHIFT_PARAM, String.valueOf(request.getInventoryOffsetShift()));
      extraParams.put(REQUEST_OLD_SRS_OFFSET_PARAM, String.valueOf(request.getOldSrsOffset()));
      extraParams.put(REQUEST_CURSOR_PARAM, String.valueOf(newCursor));
      String nextRecordId;
      if (isDeletedRecordsEnabled(request.getRequestId())) {
        nextRecordId = storageHelper.getId(getAndRemoveLastInstance(instances));
      } else {
        nextRecordId = storageHelper.getRecordId(getAndRemoveLastInstance(instances));
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
        .withExpirationDate(Instant.now().with(ChronoField.NANO_OF_SECOND, 0).plusSeconds(RESUMPTION_TOKEN_TIMEOUT))
        .withCompleteListSize(BigInteger.valueOf(totalRecords))
        .withCursor(BigInteger.valueOf(request.getCursor()));
    }

    return null;
  }

  private JsonObject getAndRemoveLastInstance(JsonArray instances) {
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
      return isNotEmpty(from) ? LocalDateTime.now().format(ISO_UTC_DATE_ONLY) : "";
    } else {
      return isNotEmpty(from) && from.matches(DATE_ONLY_PATTERN) ? LocalDateTime.now().format(ISO_UTC_DATE_ONLY)
        : "";
    }
  }

  /**
   * Filter records from source-record-storage by the fields "deleted = true" or MARC Leader 05 is "d"
   * For old behavior (repository.deletedRecords is "no") fields are checked only according to the previously described criteria.
   * In case when repository.deletedRecords is "persistent" or "transient" checked suppressedRecordsProcessing config setting
   *
   * @param request  OIA-PMH request
   * @param instance record in JsonObject format
   * @return true when a record should be present in oai-pmh response
   */
  protected boolean filterInstance(Request request, JsonObject instance) {
    if (!isDeletedRecordsEnabled(request.getRequestId())) {
      return !storageHelper.isRecordMarkAsDeleted(instance);
    } else {
      boolean shouldProcessSuppressedRecords = getBooleanProperty(request.getRequestId(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
      return shouldProcessSuppressedRecords || !storageHelper.getSuppressedFromDiscovery(instance)
        || storageHelper.isRecordMarkAsDeleted(instance);
    }
  }

  protected HeaderType addHeader(String identifierPrefix, Request request, JsonObject instance) {
    HeaderType header = populateHeader(identifierPrefix, instance, request);
    if (isDeletedRecordsEnabled(request.getRequestId()) && storageHelper.isRecordMarkAsDeleted(instance)) {
      header.setStatus(StatusType.DELETED);
    }
    return header;
  }

  /**
   * Checks if request sequences can be resumed without losing records in case of partitioning the whole result set.
   *
   * @param request      oai-pmh request
   * @param totalRecords records count into the system
   * @param instances    instances from srs
   * @return true if resumptionToken valid and request sequence can continue
   */
  protected boolean canResumeRequestSequence(Request request, Integer totalRecords, JsonArray instances) {
    Integer prevTotalRecords = request.getTotalRecords();
    boolean isDeletedRecords = isDeletedRecordsEnabled(request.getRequestId());
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

  /**
   * This method determines source from request
   * @param request instances source
   * @return FOLIO if records source is 'Inventory records source', MARC if records source is 'Source record storage',
   * null if records source is 'SRS + Inventory'
   */
  protected String resolveRequestSource(Request request) {
    var recordsSource = getProperty(request.getRequestId(), REPOSITORY_RECORDS_SOURCE);
    String source = null; // Case when SRS + Inventory.
    if (recordsSource.equals(INVENTORY)) {
      source = "FOLIO";
    } else if (recordsSource.equals(SRS)) {
      source = "MARC";
    }
    return source;
  }

  protected void saveErrorsIfExist(Request request) {
    errorsService.saveErrorsAndUpdateRequestMetadata(request.getTenant(), request.getRequestId())
      .onComplete(requestMetadataLbAsyncResult -> {
        if (requestMetadataLbAsyncResult.succeeded()) {
          var linkToErrorFile = requestMetadataLbAsyncResult.result().getLinkToErrorFile();
          if (!isBlank(linkToErrorFile)) {
            logger.info("Errors saved successfully for requestId {}", request.getRequestId());
          }
        } else {
          logger.error("Error occurred during the update of RequestMetadataLb: {}",
            requestMetadataLbAsyncResult.cause().toString());
        }
      });
  }

  @Autowired
  public void setErrorService(ErrorsService errorsService) {
    this.errorsService = errorsService;
  }

}
