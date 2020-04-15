package org.folio.oaipmh.helpers.configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;

class ConfigurationHelperTest {

  private static final String CORRECT_DIRECTORY_PATH = "config" + File.separator + "configurationHelperTestDir";
  private static final String CORRECT_JSON_FILE_NAME = "testJson";
  private static final String NONEXISTENT_DIRECTORY_PATH = "nonexistentDir";
  private static final String NONEXISTENT_JSON_FILE_NAME = "nonexistentFileName";
  private static final String JSON_FILE_WITHOUT_VALUE_FIELD = "incorrectTestJson";
  private static final String JSON_WITH_INVALID_VALUE_FIELD = "testJsonWithIncorrectValueField";
  private static final String TEST_JSON_NAME_FIELD = "name";
  private static final String TEST_JSON_VALUE_FIELD = "value";
  private static final String NAME_FILED_EXPECTED_VALUE = "testJsonName";
  private static final String VALUE_FILED_EXPECTED_VALUE = "{\"testKey\":\"testValue\"}";
  private static final String TEST_KEY = "testKey";
  private static final String TEST_VALUE = "testValue";

  private ConfigurationHelper configurationHelper = new ConfigurationHelper();

  @Test
  void shouldReturnValidJsonObject_whenGetJsonConfigFromResourcesAndParametersHaveValidValues() {
    JsonObject jsonObject = configurationHelper.getJsonConfigFromResources(CORRECT_DIRECTORY_PATH, CORRECT_JSON_FILE_NAME);
    assertEquals(NAME_FILED_EXPECTED_VALUE, jsonObject.getString(TEST_JSON_NAME_FIELD));
    assertEquals(VALUE_FILED_EXPECTED_VALUE, jsonObject.getString(TEST_JSON_VALUE_FIELD));
  }

  @Test
  void shouldThrowException_whenGetJsonConfigFromResourcesAndDirectoryPathHasIncorrectValue() {
    Exception exception = assertThrows(IllegalStateException.class, () -> {
      configurationHelper.getJsonConfigFromResources(NONEXISTENT_DIRECTORY_PATH, CORRECT_JSON_FILE_NAME);
    });
    assertThat(exception.getCause(), instanceOf(IllegalArgumentException.class));
    assertThat(exception.getMessage(), notNullValue());
  }

  @Test
  void shouldThrowException_whenGetJsonConfigFromResourcesAndJsonFileNameHasIncorrectValue() {
    Exception exception = assertThrows(IllegalStateException.class, () -> {
      configurationHelper.getJsonConfigFromResources(CORRECT_DIRECTORY_PATH, NONEXISTENT_JSON_FILE_NAME);
    });
    assertThat(exception.getCause(), instanceOf(IllegalArgumentException.class));
    assertThat(exception.getMessage(), notNullValue());
  }

  @Test
  void shouldThrowException_whenGetJsonConfigFromResourcesWithAndAllParametersAreIncorrect() {
    Exception exception = assertThrows(IllegalStateException.class, () -> {
      configurationHelper.getJsonConfigFromResources(NONEXISTENT_DIRECTORY_PATH, NONEXISTENT_JSON_FILE_NAME);
    });
    assertThat(exception.getCause(), instanceOf(IllegalArgumentException.class));
    assertThat(exception.getMessage(), notNullValue());
  }

  @Test
  void shouldReturnCorrectMap_whenGetConfigKeyValueMapFromJsonConfigEntryAndJsonEntryIsValid() {
    JsonObject jsonObject = configurationHelper.getJsonConfigFromResources(CORRECT_DIRECTORY_PATH, CORRECT_JSON_FILE_NAME);
    Map<String, String> map = configurationHelper.getConfigKeyValueMapFromJsonEntryValueField(jsonObject);
    assertTrue(map.containsKey(TEST_KEY));
    assertEquals(TEST_VALUE, map.get(TEST_KEY));
  }

  @Test
  void shouldThrowException_whenGetConfigKeyValueMapFromJsonConfigEntryAndJsonEntryHasNotValueField() {
    JsonObject jsonObject = configurationHelper.getJsonConfigFromResources(CORRECT_DIRECTORY_PATH, JSON_FILE_WITHOUT_VALUE_FIELD);
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      configurationHelper.getConfigKeyValueMapFromJsonEntryValueField(jsonObject);
    });
    assertThat(exception.getCause(), instanceOf(NullPointerException.class));
    assertThat(exception.getMessage(), notNullValue());
  }

  @Test
  void shouldThrowException_whenGetConfigKeyValueMapFromJsonConfigEntryAndJsonEntryHasInvalidValueField() {
    JsonObject jsonObject = configurationHelper.getJsonConfigFromResources(CORRECT_DIRECTORY_PATH, JSON_WITH_INVALID_VALUE_FIELD);
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      configurationHelper.getConfigKeyValueMapFromJsonEntryValueField(jsonObject);
    });
    assertThat(exception.getCause(), instanceOf(DecodeException.class));
    assertThat(exception.getMessage(), notNullValue());
  }

}
