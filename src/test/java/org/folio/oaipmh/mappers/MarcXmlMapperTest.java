package org.folio.oaipmh.mappers;

import static org.folio.oaipmh.mappers.MapperTestHelper.validateDocumentAgainstSchema;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

class MarcXmlMapperTest {

  private static final Logger logger = LogManager.getLogger(MarcXmlMapperTest.class);

  private static final String SCHEMA_FILE_PATH = "ramls/schemas/MARC21slim.xsd";

  @Test
  void correctJsonConvertingValidationTest() throws IOException {
    logger.info("=== Test correct json file converting ===");
    String input = MapperTestHelper.getStringFromFile(StaticTestRecords
        .RESOURCES_CORRECT_JSON_MARC);
    byte[] result = new MarcXmlMapper().convert(input);
    assertThat(validateDocumentAgainstSchema(result, SCHEMA_FILE_PATH), is(true));
  }

  @Test
  void incorrectJsonConvertingValidationTest() throws IOException {
    logger.info("=== Test incorrect json file converting ===");
    String input = MapperTestHelper.getStringFromFile(StaticTestRecords
        .RESOURCES_INCORRECT_JSON_MARC);
    byte[] result = new MarcXmlMapper().convert(input);
    assertThat(validateDocumentAgainstSchema(result, SCHEMA_FILE_PATH), is(false));
  }

}
