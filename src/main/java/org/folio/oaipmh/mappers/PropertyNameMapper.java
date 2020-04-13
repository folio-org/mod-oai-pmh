package org.folio.oaipmh.mappers;

import java.util.HashMap;
import java.util.Map;

/**
 * Mapper is used for mapping names of frontend property names
 * to server names.
 */
public class PropertyNameMapper {
  private static Map<String,String> mapper = new HashMap<>();

  static {
    mapper.put("deletedRecordsSupport","repository.deletedRecords");
    mapper.put("suppressedRecordsProcessing","repository.suppressedRecordsProcessing");
    mapper.put("errorsProcessing","repository.errorsProcessing");
    mapper.put("enableOaiService","repository.enableOaiService");
    mapper.put("repositoryName","repository.name");
    mapper.put("baseUrl","repository.baseURL");
    mapper.put("administratorEmail","repository.adminEmails");
    mapper.put("timeGranularity","repository.timeGranularity");
    mapper.put("maxRecordsPerResponse","repository.maxRecordsPerResponse");
    mapper.put("enableValidation","jaxb.marshaller.enableValidation");
    mapper.put("formattedOutput","jaxb.marshaller.formattedOutput");
  }

  private PropertyNameMapper(){}

  public static String mapFrontendKeyToServerKey(String keyName){
    return mapper.getOrDefault(keyName, keyName);
  }
}
