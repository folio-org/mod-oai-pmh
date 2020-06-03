package org.folio.oaipmh;

import org.folio.oaipmh.mappers.Mapper;
import org.folio.oaipmh.mappers.MarcXmlMapper;
import org.folio.oaipmh.mappers.XSLTMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Enum that represents the metadata formats supported by the repository.
 */
public enum MetadataPrefix {
  /** Refer to <a href="http://www.openarchives.org/OAI/2.0/guidelines-marcxml.htm">OAI-PMH guidelines for MARCXML</a> */
  MARC21XML("marc21",
    new MarcXmlMapper(),
    "http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd",
    "http://www.loc.gov/MARC21/slim"),
  /** Refer to <a href="https://www.openarchives.org/OAI/openarchivesprotocol.html#dublincore">Dublin Core</a> section of OAI-PMH specification */
  DC("oai_dc",
    new XSLTMapper("xslt/MARC21slim2OAIDC.xsl"),
    "http://www.openarchives.org/OAI/2.0/oai_dc.xsd",
    "http://www.openarchives.org/OAI/2.0/oai_dc/"),
  MARC21WITHHOLDINGS("marc21_withholdings",
    new MarcXmlMapper(),
    "http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd",
    "http://www.loc.gov/MARC21/slim");

  private String name;
  private Mapper mapper;
  private String schema;
  private String metadataNamespace;

  private static final Map<String, MetadataPrefix> CONSTANTS = new HashMap<>();
  private static final Set<String> FORMATS;

  static {
    for (MetadataPrefix mp : values()) {
      CONSTANTS.put(mp.name, mp);
    }
    FORMATS = Collections.unmodifiableSet(CONSTANTS.keySet());
  }

  MetadataPrefix(String name, Mapper mapper, String schema, String metadataNamespace) {
    this.name = name;
    this.mapper = mapper;
    this.schema = schema;
    this.metadataNamespace = metadataNamespace;
  }

  public static MetadataPrefix fromName(String name) {
    return CONSTANTS.get(name);
  }

  public static Set<String> getAllMetadataFormats() {
    return FORMATS;
  }

  public byte[] convert(String source) {
    return mapper.convert(source);
  }

  public String getName() {
    return name;
  }

  public String getSchema() {
    return schema;
  }

  public String getMetadataNamespace() {
    return metadataNamespace;
  }
}
