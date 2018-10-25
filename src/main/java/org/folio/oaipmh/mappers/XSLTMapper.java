package org.folio.oaipmh.mappers;

import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Templates;
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
 * This class add XSLT post-processing to transform MarcXML to desired XML format.
 */
public class XSLTMapper extends MarcXmlMapper {

  private static final String MAPPER_CREATION_ERROR_MESSAGE = "Can't create mapper with provided stylesheet.";
  private static final String MAPPER_TRANSFORMATION_ERROR_MESSAGE = "Can't transform xml.";

  private final Templates template;

  /**
   * Creates mapper with XSLT template.
   *
   * @param stylesheet path to XSLT stylesheet.
   * @throws IllegalStateException if can't create Template from provided stylesheet.
   */
  public XSLTMapper(String stylesheet) {
    try {
      InputStream inputStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(stylesheet);
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      transformerFactory.setURIResolver((href, base) -> new StreamSource(Thread.currentThread()
        .getContextClassLoader().getResourceAsStream(href)));
      template = transformerFactory.newTemplates(new StreamSource(inputStream));

    } catch (TransformerConfigurationException e) {
      throw new IllegalStateException(MAPPER_CREATION_ERROR_MESSAGE, e);
    }
  }

  /**
   * XSLT post-processing.
   *
   * @param source Node {@inheritDoc}
   * @return Node representation of XML after XSLT transformation
   */
  @Override
  public Node postProcess(Node source) {
    try {
      DocumentBuilderFactory docbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder documentBuilder = docbFactory.newDocumentBuilder();
      DOMSource domSource = new DOMSource(source);
      DOMResult result = new DOMResult(documentBuilder.newDocument());
      Transformer transformer = template.newTransformer();
      transformer.transform(domSource, result);
      return ((Document) result.getNode()).getDocumentElement();
    } catch (TransformerException | ParserConfigurationException e) {
      throw new IllegalStateException(MAPPER_TRANSFORMATION_ERROR_MESSAGE, e);
    }
  }

}
