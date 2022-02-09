package org.folio.oaipmh.processors;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.JsonEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OaiPmhJsonParserTest {

  @Test
  public void testParserHandleValidData() {

    var instances = "{  \"instanceId\": \"e6bc03c6-c137-4221-b679-a7c5c31f986c\" , \"source\": \"FOLIO\", " +
      "\"updatedDate\": \"2020-06-15T11:07:48.563Z\",  \"deleted\": \"false\",  \"suppressFromDiscovery\": \"false\"}" +
      "\n\r { \"instanceId\" : \"a7bc91c6-c137-4221-b679-a7c5c31f986c\", \"source\": \"FOLIO\"," +
      " \"updatedDate\": \"2020-06-15T11:07:48.732Z\",  \"deleted\": \"false\",  \"suppressFromDiscovery\": \"false\"}";
    var errors = new ArrayList<>();
    var events = new ArrayList<JsonEvent>();

    var oaiParser = new OaiPmhJsonParser()
      .objectValueMode()
      .handler(events::add)
      .exceptionHandler(errors::add);
    oaiParser.write(Buffer.buffer(instances)).end();
    assertEquals(0, errors.size());
    assertEquals(2, events.size());
  }

  @Test
  public void testParserHandleNotValidData() {
    var instances = "{  \"instanceId\": \"e6bc03c6-c137-4221-b679-a7c5c31f986c\" , \"source\": \"FOLIO\", " +
      "\"updatedDate\": \"2020-06-15T11:07:48.563Z\",  \"deleted\": \"false\",  \"suppressFromDiscovery\": \"false\"}" +
      "\n\r { \"instanceId\"  \"a7bc91c6-c137-4221-b679-a7c5c31f986c\", \"source\": \"FOLIO\"," +
      " \"updatedDate\": \"2020-06-15T11:07:48.732Z\",  \"deleted\": \"false\" : \"suppressFromDiscovery\": \"false\"}";

    var errors = new ArrayList<>();
    var events = new ArrayList<JsonEvent>();

    var oaiParser = new OaiPmhJsonParser()
      .objectValueMode()
      .handler(events::add)
      .exceptionHandler(errors::add);
    oaiParser.write(Buffer.buffer(instances)).end();
    assertEquals(2, errors.size());
    assertEquals(0, events.size());
  }
}
