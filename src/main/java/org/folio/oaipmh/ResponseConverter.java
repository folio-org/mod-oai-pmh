package org.folio.oaipmh;

import com.sun.xml.bind.marshaller.NamespacePrefixMapper;
import gov.loc.marc21.slim.RecordType;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.lang3.time.StopWatch;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2_0.oai_dc.Dc;
import org.openarchives.oai._2_0.oai_identifier.OaiIdentifier;
import org.purl.dc.elements._1.ObjectFactory;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI;
import static org.folio.oaipmh.Constants.JAXB_MARSHALLER_ENABLE_VALIDATION;
import static org.folio.oaipmh.Constants.JAXB_MARSHALLER_FORMATTED_OUTPUT;

@SuppressWarnings("squid:S1191") //The com.sun.xml.bind.marshaller.NamespacePrefixMapper is part of jaxb logic
public class ResponseConverter {

  private static final Logger logger = LogManager.getLogger(ResponseConverter.class);

  private static final String SCHEMA_PATH = "ramls" + File.separator + "schemas" + File.separator;
  private static final String RESPONSE_SCHEMA = SCHEMA_PATH + "OAI-PMH.xsd";
  private static final String DC_SCHEMA = SCHEMA_PATH + "oai_dc.xsd";
  private static final String SIMPLE_DC_SCHEMA = SCHEMA_PATH + "simpledc20021212.xsd";
  private static final String MARC21_SCHEMA = SCHEMA_PATH + "MARC21slim.xsd";
  private static final String OAI_IDENTIFIER_SCHEMA = SCHEMA_PATH + "oai-identifier.xsd";

  private static final Map<String, String> NAMESPACE_PREFIX_MAP = new HashMap<>();
  private final NamespacePrefixMapper namespacePrefixMapper;

  private static ResponseConverter ourInstance;

  static {
    NAMESPACE_PREFIX_MAP.put("http://www.loc.gov/MARC21/slim", "marc");
    NAMESPACE_PREFIX_MAP.put("http://purl.org/dc/elements/1.1/", "dc");
    NAMESPACE_PREFIX_MAP.put("http://www.openarchives.org/OAI/2.0/oai_dc/", "oai_dc");
    NAMESPACE_PREFIX_MAP.put("http://www.openarchives.org/OAI/2.0/oai-identifier", "oai-identifier");
    try {
      ourInstance = new ResponseConverter();
    } catch (JAXBException | SAXException e) {
      logger.error("The jaxb context could not be initialized.");
      throw new IllegalStateException("Marshaller and unmarshaller are not available.", e);
    }
  }
  private JAXBContext jaxbContext;
  private Schema oaipmhSchema;


  public static ResponseConverter getInstance() {
    return ourInstance;
  }

  /**
   * The main purpose is to initialize JAXB Marshaller and Unmarshaller to use the instances for business logic operations
   */
  private ResponseConverter() throws JAXBException, SAXException {
    jaxbContext = JAXBContext.newInstance(OAIPMH.class, RecordType.class, Dc.class, OaiIdentifier.class, ObjectFactory.class);
    // Specifying OAI-PMH schema to validate response if the validation is enabled. Enabled by default if no config specified
    if (Boolean.parseBoolean(System.getProperty(JAXB_MARSHALLER_ENABLE_VALIDATION, Boolean.TRUE.toString()))) {
      ClassLoader classLoader = this.getClass().getClassLoader();
      StreamSource[] streamSources = {
        new StreamSource(classLoader.getResourceAsStream(RESPONSE_SCHEMA)),
        new StreamSource(classLoader.getResourceAsStream(OAI_IDENTIFIER_SCHEMA)),
        new StreamSource(classLoader.getResourceAsStream(MARC21_SCHEMA)),
        new StreamSource(classLoader.getResourceAsStream(SIMPLE_DC_SCHEMA)),
        new StreamSource(classLoader.getResourceAsStream(DC_SCHEMA))
      };
      oaipmhSchema = SchemaFactory.newInstance(W3C_XML_SCHEMA_NS_URI).newSchema(streamSources);
    }
    namespacePrefixMapper = new com.sun.xml.bind.marshaller.NamespacePrefixMapper() {
      @Override
      public String getPreferredPrefix(String namespaceUri, String suggestion, boolean requirePrefix) {
        return NAMESPACE_PREFIX_MAP.getOrDefault(namespaceUri, suggestion);
      }
    };
  }

  /**
   * Marshals {@link OAIPMH} object and returns string representation
   * @param response {@link OAIPMH} object to marshal
   * @return marshaled {@link OAIPMH} object as string representation
   */
  public String convertToString(OAIPMH response) {
    var timer = StopWatch.createStarted();

    try (StringWriter writer = new StringWriter()) {
      // Marshaller is not thread-safe, so we should create every time a new one
      Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
      if (oaipmhSchema != null) {
        jaxbMarshaller.setSchema(oaipmhSchema);
      }
      // Specifying xsi:schemaLocation (which will trigger xmlns:xsi being added to RS as well)
      jaxbMarshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION,
        "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd");
      // Specifying if output should be formatted
      jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.parseBoolean(System.getProperty(JAXB_MARSHALLER_FORMATTED_OUTPUT)));
      // needed to replace the namespace prefixes with a more readable format.
      jaxbMarshaller.setProperty("com.sun.xml.bind.namespacePrefixMapper", namespacePrefixMapper);
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
   */
  public OAIPMH stringToOaiPmh(String oaipmhResponse) {
    var timer = StopWatch.createStarted();
    try (StringReader reader = new StringReader(oaipmhResponse)) {
      // Unmarshaller is not thread-safe, so we should create every time a new one
      Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
      if (oaipmhSchema != null) {
        jaxbUnmarshaller.setSchema(oaipmhSchema);
      }
      return (OAIPMH) jaxbUnmarshaller.unmarshal(reader);
    } catch (JAXBException e) {
      // In case there is an issue to unmarshal response, there is no way to handle it
      throw new IllegalStateException("The string cannot be converted to OAI-PMH response.", e);
    } finally {
      logExecutionTime("String converted to OAIPMH.", timer);
    }
  }

  /**
   * Unmarshals {@link RecordType} or {@link Dc} objects based on passed byte array
   * @param byteSource the {@link RecordType} or {@link Dc} objects in byte[] representation
   * @return the object based on passed byte array
   */
  public Object bytesToObject(byte[] byteSource) {
    var timer = StopWatch.createStarted();
    try(ByteArrayInputStream inputStream = new ByteArrayInputStream(byteSource)) {
      // Unmarshaller is not thread-safe, so we should create every time a new one
      Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
      if (oaipmhSchema != null) {
        jaxbUnmarshaller.setSchema(oaipmhSchema);
      }
      return jaxbUnmarshaller.unmarshal(inputStream);
    } catch (JAXBException | IOException e) {
      // In case there is an issue to unmarshal byteSource, there is no way to handle it
      throw new IllegalStateException("The byte array cannot be converted to JAXB object response.", e);
    } finally {
      logExecutionTime("Array of bytes converted to Object", timer);
    }
  }

  /**
   * @return Checks if the Jaxb context initialized successfully
   */
  public boolean isJaxbInitialized() {
    return jaxbContext != null;
  }

  private void logExecutionTime(final String msg, StopWatch timer) {
    timer.stop();
    logger.debug("{} after {} ms.", msg, timer.getTime());
  }
}
