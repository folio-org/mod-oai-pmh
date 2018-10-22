package org.folio.oaipmh;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
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
import java.io.StringReader;
import java.io.StringWriter;

import static javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI;

public class ResponseHelper {
  private static final Logger logger = LoggerFactory.getLogger(ResponseHelper.class);
  private static final String SCHEMA_PATH = "ramls" + File.separator + "schemas" + File.separator;
  private static final String RESPONSE_SCHEMA = SCHEMA_PATH + "OAI-PMH.xsd";
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

    // Specifying OAI-PMH schema to validate response if the validation is enabled
    if (Boolean.parseBoolean(System.getProperty("jaxb.marshaller.enableValidation"))) {
      Schema oaipmhSchema = SchemaFactory.newInstance(W3C_XML_SCHEMA_NS_URI)
                                         .newSchema(new StreamSource(this.getClass()
                                                                         .getClassLoader()
                                                                         .getResourceAsStream(RESPONSE_SCHEMA)));
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
   * @throws JAXBException can be thrown, for example, if the {@link OAIPMH} object is invalid
   */
  public String writeToString(OAIPMH response) throws JAXBException {
    StringWriter writer = new StringWriter();
    jaxbMarshaller.marshal(response, writer);

    return writer.toString();
  }

  /**
   * Unmarshals {@link OAIPMH} object based on passed string
   * @param oaipmhResponse the {@link OAIPMH} response in string representation
   * @return the {@link OAIPMH} object based on passed string
   * @throws JAXBException in case passed string is not valid OAIPMH representation
   */
  public OAIPMH stringToOaiPmh(String oaipmhResponse) throws JAXBException {
    return (OAIPMH) jaxbUnmarshaller.unmarshal(new StringReader(oaipmhResponse));
  }

  /**
   * @return Checks if the Jaxb marshaller initialized successfully
   */
  public boolean isJaxbInitialized() {
    return jaxbMarshaller != null;
  }
}
