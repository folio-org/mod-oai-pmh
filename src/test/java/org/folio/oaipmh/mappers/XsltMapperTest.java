package org.folio.oaipmh.mappers;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

class XsltMapperTest {

  private static final Logger logger = LogManager.getLogger(XsltMapperTest.class);

  private static final String SCHEMA_FILE_PATH = "ramls/schemas/oai_dc.xsd";
  private static final String INCORRECT_STYLESHEET_XSL = "incorrectStylesheet.xsl";
  private static final String XSLT_MARC21SLIM2_OAIDC_XSL = "xslt/MARC21slim2OAIDC.xsl";

  @Test
  void correctJsonConvertingValidationTest() throws IOException {
    logger.info("=== Test correct json file converting ===");
    String input = MapperTestHelper.getStringFromFile(StaticTestRecords
        .RESOURCES_CORRECT_JSON_MARC);
    byte[] result = new XsltMapper(XSLT_MARC21SLIM2_OAIDC_XSL).convert(input);
    assertThat(MapperTestHelper.validateDocumentAgainstSchema(result, SCHEMA_FILE_PATH), is(true));
  }

  @Test
  void incorrectJsonConvertingToDcValidationTest() throws IOException {
    String emptyTitle = "<dc:title/>";
    logger.info("=== Test incorrect json file converting to Dublin Core ===");
    String input = MapperTestHelper.getStringFromFile(
        StaticTestRecords.RESOURCES_INCORRECT_JSON_MARC);
    byte[] bytesResult = new XsltMapper(XSLT_MARC21SLIM2_OAIDC_XSL).convert(input);
    String result = new String(bytesResult);
    assertTrue(result.contains(emptyTitle));
  }

  @Test
  void incorrectSchemaConvertingTest() throws IOException {
    logger.info("=== Test incorrect schema file converting ===");
    String input = MapperTestHelper.getStringFromFile(
        StaticTestRecords.RESOURCES_INCORRECT_JSON_MARC);
    assertThrows(IllegalStateException.class, () -> new XsltMapper(INCORRECT_STYLESHEET_XSL)
        .convert(input));
  }
}
