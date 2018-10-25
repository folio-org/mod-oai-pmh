package org.folio.oaipmh.mappers;

import org.w3c.dom.Node;

/**
 * Converts MarcJson format to XML format.
 */
public interface Mapper {
  /**
   * Converts json string to DOM Node representation of XML document.
   *
   * @param source String representation of MarcJson source.
   * @return the Node object that represents result XML document.
   */
  Node convert(String source);
}