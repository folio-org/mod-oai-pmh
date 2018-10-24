package org.folio.oaipmh.mappers;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class XSLTMapperTest {

  private static final Logger logger = Logger.getLogger(XSLTMapperTest.class);

  private static final String SCHEMA_FILE_PATH = "ramls/schemas/oai_dc.xsd";
  private static final String INCORRECT_STYLESHEET_XSL = "incorrectStylesheet.xsl";
  private static final String XSLT_MARC21SLIM2_OAIDC_XSL = "xslt/MARC21slim2OAIDC.xsl";

  @Test
  public void correctJsonConvertingValidationTest() throws IOException {
    logger.info("=== Test correct json file converting ===");
    String input = MapperTestHelper.getStringFromFile(StaticTestRecords
      .RESOURCES_CORRECT_JSON_MARC);
    Node result = new XSLTMapper(XSLT_MARC21SLIM2_OAIDC_XSL).convert(input);
    assertTrue(MapperTestHelper.validateDocumentAgainstSchema(result, SCHEMA_FILE_PATH));
  }

  @Test
  public void incorrectJsonConvertingToDCValidationTest() throws IOException {
    logger.info("=== Test incorrect json file converting to Dublin Core ===");
    String input = MapperTestHelper.getStringFromFile(StaticTestRecords.RESOURCES_INCORRECT_JSON_MARC);
    Node result = new XSLTMapper(XSLT_MARC21SLIM2_OAIDC_XSL).convert(input);
    String nodeName = result.getFirstChild().getLocalName();
    assertEquals("title", nodeName);
    String titleValue =  result.getFirstChild().getNodeValue();
    assertNull(titleValue);
  }

  @Test(expected = IllegalStateException.class)
  public void incorrectSchemaConvertingTest() throws IOException {
    logger.info("=== Test incorrect schema file converting ===");
    String input = MapperTestHelper.getStringFromFile(StaticTestRecords.RESOURCES_INCORRECT_JSON_MARC);
    new XSLTMapper(INCORRECT_STYLESHEET_XSL).convert(input);
  }
}
