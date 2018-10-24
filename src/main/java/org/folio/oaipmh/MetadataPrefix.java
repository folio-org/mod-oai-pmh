package org.folio.oaipmh;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.folio.oaipmh.mappers.Mapper;
import org.folio.oaipmh.mappers.MarcXmlMapper;
import org.folio.oaipmh.mappers.XSLTMapper;
import org.w3c.dom.Node;


/**
 * Enum that represents the metadata formats supported by the repository.
 */
public enum MetadataPrefix {
  MARC_XML("marc_xml", new MarcXmlMapper()),
  DC("oai_dc", new XSLTMapper("xslt/MARC21slim2OAIDC.xsl"));

  private String name;
  private Mapper mapper;

  private static final Map<String, MetadataPrefix> CONSTANTS = new HashMap<>();
  private static final Set<String> FORMATS;

  static {
    for (MetadataPrefix mp : values()) {
      CONSTANTS.put(mp.name, mp);
    }
    FORMATS = Collections.unmodifiableSet(CONSTANTS.keySet());
  }

  MetadataPrefix(String name, Mapper mapper) {
    this.name = name;
    this.mapper = mapper;
  }

  public static MetadataPrefix fromName(String name) {
    return CONSTANTS.get(name);
  }

  public static Set<String> getAllMetadataFormats() {
    return FORMATS;
  }

  public Node convert(String source) {
    return mapper.convert(source);
  }

  public String getName() {
    return name;
  }
}
