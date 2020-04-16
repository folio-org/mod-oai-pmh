package org.folio.oaipmh.mappers;

import java.util.HashMap;
import java.util.Map;

/**
 * Mapper is used for mapping names of frontend property names to server names.
 */
public class PropertyNameMapper {

  private static Map<String, String> frontendToBackendMapper = new HashMap<>();
  private static Map<String, String> backendToFrontendMapper = new HashMap<>();

  static {
    frontendToBackendMapper.put("deletedRecordsSupport", "repository.deletedRecords");
    frontendToBackendMapper.put("suppressedRecordsProcessing", "repository.suppressedRecordsProcessing");
    frontendToBackendMapper.put("errorsProcessing", "repository.errorsProcessing");
    frontendToBackendMapper.put("enableOaiService", "repository.enableOaiService");
    frontendToBackendMapper.put("repositoryName", "repository.name");
    frontendToBackendMapper.put("baseUrl", "repository.baseURL");
    frontendToBackendMapper.put("administratorEmail", "repository.adminEmails");
    frontendToBackendMapper.put("timeGranularity", "repository.timeGranularity");
    frontendToBackendMapper.put("maxRecordsPerResponse", "repository.maxRecordsPerResponse");
    frontendToBackendMapper.put("enableValidation", "jaxb.marshaller.enableValidation");
    frontendToBackendMapper.put("formattedOutput", "jaxb.marshaller.formattedOutput");
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
