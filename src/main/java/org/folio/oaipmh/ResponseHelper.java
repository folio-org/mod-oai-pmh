package org.folio.oaipmh;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.time.StopWatch;
import org.openarchives.oai._2.OAIPMH;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import static javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI;

public class ResponseHelper {
  private static final Logger logger = LoggerFactory.getLogger(ResponseHelper.class);
  private static final String SCHEMA_PATH = "ramls" + File.separator + "schemas" + File.separator;
  private static final String RESPONSE_SCHEMA = SCHEMA_PATH + "OAI-PMH.xsd";
  private static final String DC_SCHEMA = SCHEMA_PATH + "oai_dc.xsd";
  private static final String SIMPLE_DC_SCHEMA = SCHEMA_PATH + "simpledc20021212.xsd";
  private static final String MARC21_SCHEMA = SCHEMA_PATH + "MARC21slim.xsd";
  private static ResponseHelper ourInstance;

  static {
    try {
      ourInstance = new ResponseHelper();
    } catch (JAXBException | SAXException e) {
      logger.error("The jaxb marshaller could not be initialized");
      throw new IllegalStateException("Marshaller is not available", e);
    }
  }

  private Marshaller jaxbMarshaller;
  private Unmarshaller jaxbUnmarshaller;

  public static ResponseHelper getInstance() {
    return ourInstance;
  }

  /**
   * The main purpose is to initialize JAXB Marshaller and Unmarshaller to use the instances for business logic operations
   */
  private ResponseHelper() throws JAXBException, SAXException {
    JAXBContext jaxbContext = JAXBContext.newInstance(OAIPMH.class);
    jaxbMarshaller = jaxbContext.createMarshaller();
    jaxbUnmarshaller = jaxbContext.createUnmarshaller();

    // Specifying OAI-PMH schema to validate response if the validation is enabled. Enabled by default if no config specified
    if (Boolean.parseBoolean(System.getProperty("jaxb.marshaller.enableValidation", Boolean.TRUE.toString()))) {
      ClassLoader classLoader = this.getClass().getClassLoader();
      StreamSource[] streamSources = {
        new StreamSource(classLoader.getResourceAsStream(RESPONSE_SCHEMA)),
        new StreamSource(classLoader.getResourceAsStream(MARC21_SCHEMA)),
        new StreamSource(classLoader.getResourceAsStream(SIMPLE_DC_SCHEMA)),
        new StreamSource(classLoader.getResourceAsStream(DC_SCHEMA))
      };
      Schema oaipmhSchema = SchemaFactory.newInstance(W3C_XML_SCHEMA_NS_URI).newSchema(streamSources);
      jaxbMarshaller.setSchema(oaipmhSchema);
      jaxbUnmarshaller.setSchema(oaipmhSchema);
    }

    // Specifying xsi:schemaLocation (which will trigger xmlns:xsi being added to RS as well)
    jaxbMarshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION,
      "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd");

    // Specifying if output should be formatted
    jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.parseBoolean(System.getProperty("jaxb.marshaller.formattedOutput")));
  }

  /**
   * Marshals {@link OAIPMH} object and returns string representation
   * @param response {@link OAIPMH} object to marshal
   * @return marshaled {@link OAIPMH} object as string representation
   */
  public String writeToString(OAIPMH response) {
    StopWatch timer = logger.isDebugEnabled() ? StopWatch.createStarted() : null;
    try (StringWriter writer = new StringWriter()) {
      jaxbMarshaller.marshal(response, writer);
      return writer.toString();
    } catch (JAXBException | IOException e) {
      // In case there is an issue to marshal response, there is no way to handle it
      throw new IllegalStateException("The OAI-PMH response cannot be converted to string representation.", e);
    } finally {
      logExecutionTime("OAIPMH converted to string", timer);
    }
  }

  /**
   * Unmarshals {@link OAIPMH} object based on passed string
   * @param oaipmhResponse the {@link OAIPMH} response in string representation
   * @return the {@link OAIPMH} object based on passed string
   * @throws JAXBException in case passed string is not valid OAIPMH representation
   */
  public OAIPMH stringToOaiPmh(String oaipmhResponse) throws JAXBException {
    StopWatch timer = logger.isDebugEnabled() ? StopWatch.createStarted() : null;
    try (StringReader reader = new StringReader(oaipmhResponse)) {
      return (OAIPMH) jaxbUnmarshaller.unmarshal(reader);
    } finally {
      logExecutionTime("String converted to OAIPMH", timer);
    }
  }

  /**
   * @return Checks if the Jaxb marshaller initialized successfully
   */
  public boolean isJaxbInitialized() {
    return jaxbMarshaller != null;
  }

  private void logExecutionTime(final String msg, StopWatch timer) {
    if (timer != null) {
      timer.stop();
      logger.debug(String.format("%s after %d ms", msg, timer.getTime()));
    }
  }
}
