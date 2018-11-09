package org.folio.oaipmh.mappers;

import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class MapperTestHelper {

  static String getStringFromFile(String path) throws IOException {
    File file = new File(MarcXmlMapperTest.class.getResource(path).getFile());
    byte[] encoded = Files.readAllBytes(Paths.get(file.getPath()));
    return new String(encoded, StandardCharsets.UTF_8);
  }

  static boolean validateDocumentAgainstSchema(byte[] byteSource, String shemaFilePath) {
    try {
      SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      URL schemaURL = new File(shemaFilePath).toURI().toURL();
      Schema schema = sf.newSchema(schemaURL);
      Validator validator = schema.newValidator();
      Source source = new StreamSource(new ByteArrayInputStream(byteSource));
      validator.validate(source);
      return true;
    } catch (SAXException | IOException e) {
      return false;
    }
  }
}
