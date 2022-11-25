package org.folio.rest.impl;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.ResponseConverter;
import org.folio.oaipmh.helpers.configuration.ConfigurationHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import java.util.Map;

@ExtendWith(VertxExtension.class)
class InitAPIsTest {

  private final Logger logger = LogManager.getLogger(this.getClass());

  private static final String CONFIGURATION_PATH = "configuration.path";
  private static final String CONFIGURATION_FILES = "configuration.files";

  private static final String CORRECT_TEST_CONFIGURATION_PATH = "config" + System.getProperty("file.separator") + "test-config";
  private static final String INCORRECT_TEST_CONFIGURATION_PATH = "nonexistentDirectoryPath";

  private static final String CORRECT_TEST_CONFIGURATION_FILES = "test1.json,test2.json";
  private static final String INCORRECT_TEST_CONFIGURATION_FILES = "nonexistent1.json,nonexistent2.json";

  private static final String FIRST_TEST_PROPERTY_NAME = "testProperty1";
  private static final String FIRST_TEST_PROPERTY_VALUE = "testValue1";
  private static final String SECOND_TEST_PROPERTY_NAME = "testProperty2";
  private static final String SECOND_TEST_PROPERTY_VALUE = "testValue2";

  private static final String BEHAVIOUR_GROUP_TEST_CONFIG = "repository.deletedRecords";
  private static final String GENERAL_GROUP_TEST_CONFIG = "repository.baseURL";
  private static final String TECHNICAL_GROUP_TEST_CONFIG = "repository.maxRecordsPerResponse";

  @BeforeEach
  void setUp() {
    System.setProperty(CONFIGURATION_PATH, CORRECT_TEST_CONFIGURATION_PATH);
    System.setProperty(CONFIGURATION_FILES, CORRECT_TEST_CONFIGURATION_FILES);
  }

  @AfterAll
  static void tearDown() {
    System.clearProperty(CONFIGURATION_PATH);
    System.clearProperty(CONFIGURATION_FILES);
  }

  @Test
  void shouldInitSuccessfully_whenConfigFilePathAndConfigFilesPropertiesHaveCorrectValues(Vertx vertx,
      VertxTestContext testContext) {
    logger.info("run shouldInitSuccessfully_whenConfigFilePathAndConfigFilesPropertiesHaveCorrectValues");
    new InitAPIs().init(vertx, vertx.getOrCreateContext(), testContext.succeeding(result -> {
      assertTrue(result);
      assertEquals(FIRST_TEST_PROPERTY_VALUE, System.getProperty(FIRST_TEST_PROPERTY_NAME));
      assertEquals(SECOND_TEST_PROPERTY_VALUE, System.getProperty(SECOND_TEST_PROPERTY_NAME));
      verifyJaxbInitialized();
      logger.info("shouldInitSuccessfully_whenConfigFilePathAndConfigFilesPropertiesHaveCorrectValues finished");
      testContext.completeNow();
    }));
  }

  @Test
  void shouldInitSuccessfully_whenDefaultConfigFilePathAndConfigFilesPropertyValuesAreUsed(Vertx vertx, VertxTestContext testContext) {
    System.clearProperty(CONFIGURATION_PATH);
    System.clearProperty(CONFIGURATION_FILES);
    logger.info("run shouldInitSuccessfully_whenDefaultConfigFilePathAndConfigFilesPropertyValuesAreUsed");
    new InitAPIs().init(vertx, vertx.getOrCreateContext(), testContext.succeeding(result -> {
      assertTrue(result);
      assertNotNull(System.getProperty(BEHAVIOUR_GROUP_TEST_CONFIG));
      assertNotNull(System.getProperty(GENERAL_GROUP_TEST_CONFIG));
      assertNotNull(System.getProperty(TECHNICAL_GROUP_TEST_CONFIG));
      verifyJaxbInitialized();
      logger.info("shouldInitSuccessfully_whenDefaultConfigFilePathAndConfigFilesPropertyValuesAreUsed finished");
      testContext.completeNow();
    }));
  }

  @Test
  void shouldAddRecordsSourceDefaultValueSuccessfully_whenDefaultConfigFilePathAndConfigFilesPropertyValuesAreUsed(Vertx vertx, VertxTestContext testContext) {
    System.clearProperty(CONFIGURATION_PATH);
    System.clearProperty(CONFIGURATION_FILES);
    new InitAPIs().init(vertx, vertx.getOrCreateContext(), testContext.succeeding(result -> {
      assertTrue(result);
      JsonObject jsonConfigBehavior = ConfigurationHelper.getInstance().getJsonConfigFromResources("config", "behavior.json");
      assertEquals("Source record storage", new JsonObject(jsonConfigBehavior.getString("value")).getString("recordsSource"));
      verifyJaxbInitialized();
      testContext.completeNow();
    }));
  }

  @Test
  void shouldInitWithFailure_whenConfigFilePathPropertyHasIncorrectValue(Vertx vertx, VertxTestContext testContext) {
    logger.info("run shouldInitWithFailure_whenConfigFilePathPropertyHasIncorrectValue");
    System.setProperty(CONFIGURATION_PATH, INCORRECT_TEST_CONFIGURATION_PATH);
    new InitAPIs().init(vertx, vertx.getOrCreateContext(), testContext.failing(throwable -> {
      assertTrue(throwable instanceof IllegalStateException);
      assertTrue(throwable.getMessage()
        .contains("Unable open the resource file"));
      verifyJaxbInitialized();
      logger.info("shouldInitWithFailure_whenConfigFilePathPropertyHasIncorrectValue finished");
      testContext.completeNow();
    }));
  }

  @Test
  void shouldInitWithFailure_whenFilesOfConfigFilesPropertyAreNotExist(Vertx vertx, VertxTestContext testContext) {
    logger.info("run shouldInitWithFailure_whenFilesOfConfigFilesPropertyAreNotExist");
    System.setProperty(CONFIGURATION_FILES, INCORRECT_TEST_CONFIGURATION_FILES);
    new InitAPIs().init(vertx, vertx.getOrCreateContext(), testContext.failing(throwable -> {
      assertTrue(throwable instanceof IllegalStateException);
      assertTrue(throwable.getMessage()
        .contains("Unable open the resource file"));
      verifyJaxbInitialized();
      logger.info("shouldInitWithFailure_whenFilesOfConfigFilesPropertyAreNotExist finished");
      testContext.completeNow();
    }));
  }

  @Test
  void shouldInitWithFailure_whenConfigFilesPropertyHasEmptyFileNamesList(Vertx vertx, VertxTestContext testContext) {
    logger.info("run shouldInitWithFailure_whenConfigFilesPropertyHasEmptyFileNamesList");
    System.setProperty(CONFIGURATION_FILES, EMPTY);
    new InitAPIs().init(vertx, vertx.getOrCreateContext(), testContext.failing(throwable -> {
      assertTrue(throwable instanceof DecodeException);
      verifyJaxbInitialized();
      logger.info("shouldInitWithFailure_whenConfigFilesPropertyHasEmptyFileNamesList finished");
      testContext.completeNow();
    }));
  }

  private void verifyJaxbInitialized() {
    assertTrue(ResponseConverter.getInstance()
      .isJaxbInitialized());
  }

}
