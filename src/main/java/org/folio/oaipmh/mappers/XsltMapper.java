package org.folio.oaipmh.mappers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.xml.XMLConstants;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class add XSLT post-processing to transform MarcXML to desired XML format.
 */
public class XsltMapper extends MarcXmlMapper {

  private static final Logger logger = LogManager.getLogger(XsltMapper.class);

  private static final String MAPPER_CREATION_ERROR_MESSAGE =
      "Can't create mapper with provided stylesheet.";
  private static final String MAPPER_TRANSFORMATION_ERROR_MESSAGE = "Can't transform xml.";

  private final Templates template;

  /**
   * Creates mapper with XSLT template.
   *
   * @param stylesheet path to XSLT stylesheet.
   * @throws IllegalStateException if can't create Template from provided stylesheet.
   */
  public XsltMapper(String stylesheet) {
    try {
      InputStream inputStream = Thread.currentThread().getContextClassLoader()
          .getResourceAsStream(stylesheet);
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      transformerFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      transformerFactory.setURIResolver((href, base) ->
          new StreamSource(Thread.currentThread()
              .getContextClassLoader().getResourceAsStream(href)));
      template = transformerFactory.newTemplates(new StreamSource(inputStream));

    } catch (TransformerConfigurationException e) {
      throw new IllegalStateException(MAPPER_CREATION_ERROR_MESSAGE, e);
    }
  }

  /**
   * Convert MarcJson to MarcXML with XSLT post-processing.
   *
   * @param source {@inheritDoc}
   * @return byte[] representation of XML after XSLT transformation
   */
  @Override
  public byte[] convert(String source) {
    byte[] marcXmlResult = super.convert(source);
    StopWatch timer = logger.isDebugEnabled() ? StopWatch.createStarted() : null;
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      Transformer transformer = template.newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      transformer.transform(new StreamSource(new ByteArrayInputStream(marcXmlResult)),
          new StreamResult(out));
      return out.toByteArray();
    } catch (TransformerException | IOException e) {
      throw new IllegalStateException(MAPPER_TRANSFORMATION_ERROR_MESSAGE, e);
    } finally {
      if (timer != null) {
        timer.stop();
        logger.debug("MarcXml converted to other format by XSLT transformation after {} ms.",
            timer.getTime());
      }
    }
  }

}
