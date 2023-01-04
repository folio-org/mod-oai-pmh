package org.folio.oaipmh;

import java.time.format.DateTimeFormatter;

public final class Constants {

  private Constants() {
    throw new IllegalStateException("This class holds constants only");
  }


  /**
   * Strict ISO Date and Time with UTC offset.
   * Represents {@linkplain org.openarchives.oai._2.GranularityType#YYYY_MM_DD_THH_MM_SS_Z YYYY_MM_DD_THH_MM_SS_Z} granularity
   */
  public static final String ISO_DATE_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss'Z'";
  public static final String ISO_DATE_ONLY_PATTERN = "yyyy-MM-dd";
  public static final DateTimeFormatter ISO_UTC_DATE_TIME = DateTimeFormatter.ofPattern(ISO_DATE_TIME_PATTERN);
  public static final DateTimeFormatter ISO_UTC_DATE_ONLY = DateTimeFormatter.ofPattern(ISO_DATE_ONLY_PATTERN);

  public static final String OKAPI_URL = "X-Okapi-Url";
  public static final String OKAPI_TENANT = "X-Okapi-Tenant";
  public static final String OKAPI_TOKEN = "X-Okapi-Token";

  public static final String JAXB_MARSHALLER_FORMATTED_OUTPUT = "jaxb.marshaller.formattedOutput";
  public static final String JAXB_MARSHALLER_ENABLE_VALIDATION = "jaxb.marshaller.enableValidation";
  public static final String REPOSITORY_BASE_URL = "repository.baseURL";
  public static final String REPOSITORY_MAX_RECORDS_PER_RESPONSE = "repository.maxRecordsPerResponse";
  public static final String REPOSITORY_SRS_HTTP_REQUEST_RETRY_ATTEMPTS = "repository.srsHttpRequestRetryAttempts";
  public static final String REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC = "repository.srsClientIdleTimeoutSec";
  public static final String REPOSITORY_NAME = "repository.name";
  public static final String REPOSITORY_ADMIN_EMAILS = "repository.adminEmails";
  public static final String REPOSITORY_TIME_GRANULARITY = "repository.timeGranularity";
  public static final String REPOSITORY_DELETED_RECORDS = "repository.deletedRecords";
  public static final String REPOSITORY_STORAGE = "repository.storage";
  public static final String REPOSITORY_PROTOCOL_VERSION_2_0 = "2.0";
  public static final String REPOSITORY_SUPPRESSED_RECORDS_PROCESSING = "repository.suppressedRecordsProcessing";
  public static final String REPOSITORY_ENABLE_OAI_SERVICE = "repository.enableOaiService";
  public static final String REPOSITORY_ERRORS_PROCESSING = "repository.errorsProcessing";
  public static final String REPOSITORY_FETCHING_CHUNK_SIZE = "repository.fetchingChunkSize";
  public static final String REPOSITORY_RECORDS_SOURCE = "repository.recordsSource";

  public static final String SRS_AND_INVENTORY = "Source record storage and Inventory";
  public static final String INVENTORY = "Inventory";

  public static final String SOURCE_RECORD_STORAGE = "SRS";
  public static final String INVENTORY_RECORD_STORAGE = "INVENTORY";

  public static final String PARSED_RECORD = "parsedRecord";
  public static final String CONTENT = "content";
  public static final String FIELDS = "fields";
  public static final String SUBFIELDS = "subfields";
  public static final String FIRST_INDICATOR = "ind1";
  public static final String SECOND_INDICATOR = "ind2";

  public static final String CONFIGS = "configs";
  public static final String VALUE = "value";

  public static final String EXPIRATION_DATE_RESUMPTION_TOKEN_PARAM = "expirationDate";
  public static final String FROM_PARAM = "from";
  public static final String IDENTIFIER_PARAM = "identifier";
  public static final String METADATA_PREFIX_PARAM = "metadataPrefix";
  public static final String RESUMPTION_TOKEN_PARAM = "resumptionToken";
  public static final String SET_PARAM = "set";
  public static final String UNTIL_PARAM = "until";
  public static final String OFFSET_PARAM = "offset";
  public static final String TOTAL_RECORDS_PARAM = "totalRecords";
  public static final String NEXT_RECORD_ID_PARAM = "nextRecordId";
  public static final String NEXT_INSTANCE_PK_VALUE = "nextInstancePkValue";
  public static final String REQUEST_ID_PARAM = "requestId";
  public static final String VERB_PARAM = "verb";

  public static final String DEFLATE = "deflate";
  public static final String GZIP = "gzip";

  public static final String GENERIC_ERROR_MESSAGE = "Sorry, we can't process your request. Please contact administrator(s).";
  public static final String CANNOT_DISSEMINATE_FORMAT_ERROR = "The value of the MetadataPrefix argument is not supported by the repository";
  public static final String RESUMPTION_TOKEN_FORMAT_ERROR = "The value of the resumptionToken argument is invalid";
  public static final String RESUMPTION_TOKEN_FLOW_ERROR = "There were substantial changes to the repository and continuing may result in missing records.";
  public static final String LIST_NO_REQUIRED_PARAM_ERROR = "Missing required parameters: metadataPrefix";
  public static final String LIST_ILLEGAL_ARGUMENTS_ERROR = "Verb '%s', argument 'resumptionToken' is exclusive, no others maybe specified with it.";
  public static final String INVALID_RESUMPTION_TOKEN = "Verb '%s', argument resumptionToken is invalid";
  public static final String NO_RECORD_FOUND_ERROR = "There is no any record found matching search criteria";
  public static final String BAD_DATESTAMP_FORMAT_ERROR = "Bad datestamp format for '%s=%s' argument.";
  public static final String RECORD_METADATA_PREFIX_PARAM_ERROR = "The request is missing required arguments. There is no metadataPrefix.";
  public static final String RECORD_NOT_FOUND_ERROR = "No matching identifier in repository.";
  public static final String INVALID_IDENTIFIER_ERROR_MESSAGE = "Identifier has invalid structure.";
  public static final String EXPIRED_RESUMPTION_TOKEN = "The value of the resumptionToken argument is expired";

  public static final String SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE = "Field '%s' cannot be empty or null";

  public static final String LOCATION_URI = "/locations";
  public static final String ILL_POLICIES_URI = "/ill-policies";
  public static final String MATERIAL_TYPES_URI = "/material-types";
  public static final String RESOURCE_TYPES_URI = "/instance-types";
  public static final String INSTANCE_FORMATS_URI = "/instance-formats";

  public static final String LOCATION = "location";
  public static final String ILL_POLICIES = "illPolicy";
  public static final String MATERIAL_TYPES = "materialType";
  public static final String INSTANCE_TYPES = "resourceType";
  public static final String INSTANCE_FORMATS = "format";
  public static final String RETRY_ATTEMPTS = "retryAttempts";
  public static final String STATUS_CODE = "statusCode";
  public static final String STATUS_MESSAGE = "statusMessage";

  public static final String SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS = "skipSuppressedFromDiscoveryRecords";
  public static final String INSTANCE_ID_FIELD_NAME = "instanceId";
  public static final String SUPPRESS_FROM_DISCOVERY = "suppressFromDiscovery";
  public static final String INVENTORY_STORAGE = "inventory-storage";

  public static final Integer RESUMPTION_TOKEN_TIMEOUT = 24 * 60 * 60;
}
