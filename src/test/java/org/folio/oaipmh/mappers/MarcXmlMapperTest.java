package org.folio.oaipmh.mappers;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

import java.io.IOException;

import static org.folio.oaipmh.mappers.MapperTestHelper.validateDocumentAgainstSchema;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MarcXmlMapperTest {

  private static final Logger logger = Logger.getLogger(MarcXmlMapperTest.class);

  private static final String SCHEMA_FILE_PATH = "ramls/schemas/MARC21slim.xsd";

  @Test
  public void correctJsonConvertingValidationTest() throws IOException {
    logger.info("=== Test correct json file converting ===");
    String input = MapperTestHelper.getStringFromFile(StaticTestRecords.RESOURCES_CORRECT_JSON_MARC);
    Node result = new MarcXmlMapper().convert(input);
    assertThat(validateDocumentAgainstSchema(result, SCHEMA_FILE_PATH), is(true));
  }

  @Test
  public void incorrectJsonConvertingValidationTest() throws IOException {
    logger.info("=== Test incorrect json file converting ===");
    String input = MapperTestHelper.getStringFromFile(StaticTestRecords.RESOURCES_INCORRECT_JSON_MARC);
    Node result = new MarcXmlMapper().convert(input);
    assertThat(validateDocumentAgainstSchema(result, SCHEMA_FILE_PATH), is(false));
  }

}
