package org.folio.rest.utils;

import org.w3c.dom.Node;

/**
 * Converts MarkJson format to XML format.
 */
public interface Mapper {
  /**
   * Converts json string to DOM Node representation of XML document.
   *
   * @param source String representation of MarkJson source.
   * @return the Node object that represents result XML document.
   */
  Node convert(String source);
}
