package org.folio.oaipmh.mappers;

/**
 * Converts MarcJson format to XML format.
 */
public interface Mapper {
  /**
   * Converts json string to byte[] representation of XML document.
   *
   * @param source String representation of MarcJson source.
   * @return byte[] that represents result XML document.
   */
  byte[] convert(String source);
}
