package org.folio.rest.utils;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.dom.DOMSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MarcJsonToMarcXmlMapperTest {

  private static final Logger logger = Logger.getLogger(MarcJsonToMarcXmlMapperTest.class);

  private static final String SCHEMA_FILE = "ramls/schemas/MARC21slim.xsd";

  @Test
  public void correctJsonConvertingValidationTest() throws IOException {
    logger.info("=== Test correct json file converting ===");
    String input = getStringFromFile(StaticTestRecords.RESOURCES_CORRECT_JSON_MARC);
    Node result = new MarcXmlMapper().convert(input);
    assertTrue(validateDocumentAgainstSchema(result));
  }

  @Test
  public void incorrectJsonConvertingValidationTest() throws IOException {
    logger.info("=== Test incorrect json file converting ===");
    String input = getStringFromFile(StaticTestRecords.RESOURCES_INCORRECT_JSON_MARC);
    Node result = new MarcXmlMapper().convert(input);
    assertFalse(validateDocumentAgainstSchema(result));
  }

  private String getStringFromFile(String path) throws IOException {
    File file = new File(MarcJsonToMarcXmlMapperTest.class.getResource(path).getFile());
    byte[] encoded = Files.readAllBytes(Paths.get(file.getPath()));
    return new String(encoded, StandardCharsets.UTF_8);
  }

  private boolean validateDocumentAgainstSchema(Node node) {
    try {
      SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      URL schemaURL = new File(SCHEMA_FILE).toURI().toURL();
      Schema schema = sf.newSchema(schemaURL);
      Validator validator = schema.newValidator();
      DOMSource source = new DOMSource(node);
      validator.validate(source);
      return true;
    } catch (SAXException | IOException e) {
      return false;
    }
  }
}
