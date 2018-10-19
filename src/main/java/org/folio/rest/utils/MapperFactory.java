package org.folio.rest.utils;

import org.folio.oaipmh.MetadataPrefix;

/**
 * Factory class to create suitable mapper based on {@link MetadataPrefix}.
 */
public class MapperFactory {

  public Mapper getMapper(MetadataPrefix metadataPrefix) {
    if (MetadataPrefix.MARC_XML == metadataPrefix) {
      return new MarcXmlMapper();
    } else {
      return new XSLTMapper();
    }
  }
}
