package org.folio.rest.utils;

import org.apache.log4j.Logger;
import org.folio.oaipmh.mappers.MarcXmlMapper;
import org.junit.Test;
import org.w3c.dom.Node;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MarcXmlMapperTest {

  private static final Logger logger = Logger.getLogger(MarcXmlMapperTest.class);

  private static final String SCHEMA_FILE_PATH = "ramls/schemas/MARC21slim.xsd";

  @Test
  public void correctJsonConvertingValidationTest() throws IOException {
    logger.info("=== Test correct json file converting ===");
    String input = MapperTestHelper.getStringFromFile(StaticTestRecords.RESOURCES_CORRECT_JSON_MARC);
    Node result = new MarcXmlMapper().convert(input);
    assertTrue(MapperTestHelper.validateDocumentAgainstSchema(result, SCHEMA_FILE_PATH));
  }

  @Test
  public void incorrectJsonConvertingValidationTest() throws IOException {
    logger.info("=== Test incorrect json file converting ===");
    String input = MapperTestHelper.getStringFromFile(StaticTestRecords.RESOURCES_INCORRECT_JSON_MARC);
    Node result = new MarcXmlMapper().convert(input);
    assertFalse(MapperTestHelper.validateDocumentAgainstSchema(result, SCHEMA_FILE_PATH));
  }

}
