package org.folio.rest.utils;

import org.w3c.dom.Node;

/**
 * Converts MarkJson format to MarcXML format.
 */
public class MarcXmlMapper extends AbstractMapper {

  /**
   * {@inheritDoc}
   *
   * @param source String representation of MarkJson source.
   * @return the Node object that represents result MarcXML document
   */
  @Override
  public Node convert(String source) {
    return templateConvert(source);
  }

}
