package org.folio.rest.utils;

import org.folio.oaipmh.MetadataPrefix;

public class MapperFactory {

  public Mapper getMapper(MetadataPrefix metadataPrefix) {
    if (MetadataPrefix.MARC_XML.equals(metadataPrefix)) {
      return new MarcXmlMapper();
    } else {
      return  new XSLTMapper();
    }
  }
}
