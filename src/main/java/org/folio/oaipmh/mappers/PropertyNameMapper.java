package org.folio.oaipmh.mappers;

import java.util.HashMap;
import java.util.Map;
import org.folio.oaipmh.Constants;

import static org.folio.oaipmh.Constants.JAXB_MARSHALLER_ENABLE_VALIDATION;
import static org.folio.oaipmh.Constants.JAXB_MARSHALLER_FORMATTED_OUTPUT;
import static org.folio.oaipmh.Constants.REPOSITORY_ADMIN_EMAILS;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;
import static org.folio.oaipmh.Constants.REPOSITORY_ENABLE_OAI_SERVICE;
import static org.folio.oaipmh.Constants.REPOSITORY_ERRORS_PROCESSING;
import static org.folio.oaipmh.Constants.REPOSITORY_FETCHING_CHUNK_SIZE;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_NAME;
import static org.folio.oaipmh.Constants.REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC;
import static org.folio.oaipmh.Constants.REPOSITORY_SRS_HTTP_REQUEST_RETRY_ATTEMPTS;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.REPOSITORY_TIME_GRANULARITY;

/**
 * Mapper is used for mapping names of frontend property names to server names.
 */
public class PropertyNameMapper {

  private static Map<String, String> frontendToBackendMapper = new HashMap<>();
  private static Map<String, String> backendToFrontendMapper = new HashMap<>();

  static {
    frontendToBackendMapper.put("deletedRecordsSupport", REPOSITORY_DELETED_RECORDS);
    frontendToBackendMapper.put("suppressedRecordsProcessing", REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
    frontendToBackendMapper.put("errorsProcessing", REPOSITORY_ERRORS_PROCESSING);
    frontendToBackendMapper.put("enableOaiService", REPOSITORY_ENABLE_OAI_SERVICE);
    frontendToBackendMapper.put("repositoryName", REPOSITORY_NAME);
    frontendToBackendMapper.put("baseUrl", REPOSITORY_BASE_URL);
    frontendToBackendMapper.put("administratorEmail", REPOSITORY_ADMIN_EMAILS);
    frontendToBackendMapper.put("timeGranularity", REPOSITORY_TIME_GRANULARITY);
    frontendToBackendMapper.put("maxRecordsPerResponse", REPOSITORY_MAX_RECORDS_PER_RESPONSE);
    frontendToBackendMapper.put("enableValidation", JAXB_MARSHALLER_ENABLE_VALIDATION);
    frontendToBackendMapper.put("formattedOutput", JAXB_MARSHALLER_FORMATTED_OUTPUT);
    frontendToBackendMapper.put("srsHttpRequestRetryAttempts", REPOSITORY_SRS_HTTP_REQUEST_RETRY_ATTEMPTS);
    frontendToBackendMapper.put("srsClientIdleTimeoutSec", REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC);
    frontendToBackendMapper.put("fetchingChunkSize", REPOSITORY_FETCHING_CHUNK_SIZE);
    frontendToBackendMapper.forEach((key, value) -> backendToFrontendMapper.put(value, key));
  }

  private PropertyNameMapper() {
  }

  public static String mapFrontendKeyToServerKey(String keyName) {
    return frontendToBackendMapper.getOrDefault(keyName, keyName);
  }

  public static String mapToFrontendKeyName(String backendKeyName) {
    return backendToFrontendMapper.get(backendKeyName);
  }
}
