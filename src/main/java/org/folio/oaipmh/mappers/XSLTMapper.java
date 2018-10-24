package org.folio.oaipmh.mappers;

import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Document;
import org.w3c.dom.Node;


/**
 * Converts MarkJson format to xml format.
 */
public class XSLTMapper extends AbstractMapper {

  private static final String MAPPER_CREATION_ERROR_MESSAGE = "Can't create mapper with provided stylesheet.";
  private static final String MAPPER_TRANSFORMATION_ERROR_MESSAGE = "Can't transform xml.";

  private final Transformer transformer;
  private final DocumentBuilder documentBuilder;

  /**
   * Creates mapper with XSLT transformer.
   *
   * @param stylesheet path to XSLT stylesheet.
   * @throws IllegalStateException if can't create Transformer from provided stylesheet.
   */
  public XSLTMapper(String stylesheet) {
    try {
      InputStream inputStream = Thread.currentThread().getContextClassLoader()
          .getResourceAsStream(stylesheet);
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      transformerFactory.setURIResolver((href, base) -> new StreamSource(Thread.currentThread()
          .getContextClassLoader().getResourceAsStream(href)));
      transformer = transformerFactory.newTransformer(new StreamSource(inputStream));
      DocumentBuilderFactory docbFactory = DocumentBuilderFactory.newInstance();
      documentBuilder = docbFactory.newDocumentBuilder();
    } catch (TransformerConfigurationException | ParserConfigurationException e) {
      throw new IllegalStateException(MAPPER_CREATION_ERROR_MESSAGE, e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param source String representation of MarkJson source.
   * @return {@inheritDoc}
   */
  @Override
  public Node convert(String source) {
    DOMSource domSource = new DOMSource(templateConvert(source));
    DOMResult result = new DOMResult(documentBuilder.newDocument());
    try {
      transformer.transform(domSource, result);
      return ((Document) result.getNode()).getDocumentElement();
    } catch (TransformerException e) {
      throw new IllegalStateException(MAPPER_TRANSFORMATION_ERROR_MESSAGE, e);
    }
  }

}
