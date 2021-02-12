package org.folio.oaipmh.mappers;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.folio.oaipmh.mappers.MapperTestHelper.validateDocumentAgainstSchema;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class MarcXmlMapperTest {

  private static final Logger logger = LoggerFactory.getLogger(MarcXmlMapperTest.class);

  private static final String SCHEMA_FILE_PATH = "ramls/schemas/MARC21slim.xsd";

  @Test
  void correctJsonConvertingValidationTest() throws IOException {
    logger.info("=== Test correct json file converting ===");
    String input = MapperTestHelper.getStringFromFile(StaticTestRecords.RESOURCES_CORRECT_JSON_MARC);
    byte[] result = new MarcXmlMapper().convert(input);
    assertThat(validateDocumentAgainstSchema(result, SCHEMA_FILE_PATH), is(true));
  }

  @Test
  void incorrectJsonConvertingValidationTest() throws IOException {
    logger.info("=== Test incorrect json file converting ===");
    String input = MapperTestHelper.getStringFromFile(StaticTestRecords.RESOURCES_INCORRECT_JSON_MARC);
    byte[] result = new MarcXmlMapper().convert(input);
    assertThat(validateDocumentAgainstSchema(result, SCHEMA_FILE_PATH), is(false));
  }

}
