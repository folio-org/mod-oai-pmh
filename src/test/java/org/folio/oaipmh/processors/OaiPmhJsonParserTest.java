package org.folio.oaipmh.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.JsonEvent;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OaiPmhJsonParserTest {

  private static final Logger log = LoggerFactory.getLogger(OaiPmhJsonParserTest.class);

  @Test
 void testParserHandleValidData() {
    var instances = "{  \"instanceId\": \"e6bc03c6-c137-4221-b679-a7c5c31f986c\" , \"source\": "
        + "\"FOLIO\", \"updatedDate\": \"2020-06-15T11:07:48.563Z\",  \"deleted\": \"false\",  "
        + "\"suppressFromDiscovery\": \"false\"}\n\r { \"instanceId\" : \"a7bc91c6-c137-4221-"
        + "b679-a7c5c31f986c\", \"source\": \"FOLIO\",\"updatedDate\": \"2020-06-15T11:07:48."
        + "732Z\",  \"deleted\": \"false\",  \"suppressFromDiscovery\": \"false\"}";
    var errors = new ArrayList<>();
    var events = new ArrayList<JsonEvent>();

    var jsonParser = new OaiPmhJsonParser()
        .objectValueMode()
        .handler(events::add)
        .exceptionHandler(errors::add);
    jsonParser.write(Buffer.buffer(instances)).end();
    assertEquals(0, errors.size());
    assertEquals(2, events.size());
  }

  @Test
  void testParserHandleInternalError() {
    var instances = "{  \"instanceId\": \"e6bc03c6-c137-4221-b679-a7c5c31f986c\" , \"source\": "
        + "\"FOLIO\", \"updatedDate\": \"2020-06-15T11:07:48.563Z\",  \"deleted\": \"false\",  "
        + "\"suppressFromDiscovery\": \"false\"}\n\r { \"instanceId\" : \"a7bc91c6-c137-4221-"
        + "b679-a7c5c31f986c\", \"source\": \"FOLIO\", \"updatedDate\": \"2020-06-15T11:07:48."
        + "732Z\",  \"deleted\": \"false\",  \"suppressFromDiscovery\": \"false\"}";

    var jsonParser = new OaiPmhJsonParser()
        .objectValueMode()
        .handler(handler -> {
          throw new ClassCastException("class java.lang.String cannot be cast to class "
              + "io.vertx.core.json.JsonObject (java.lang.String is in module java.base of "
              + "loader 'bootstrap'; io.vertx.core.json.JsonObject is in unnamed module of "
              + "loader 'app')");
        })
        .exceptionHandler(handler -> log.error("Error parsing"));
    jsonParser.write(Buffer.buffer(instances)).end();
    assertEquals(1, ((OaiPmhJsonParser) jsonParser).getErrors().size());
  }

  @Test
  void testParserHandleNotValidData() {
    var json = "{  \"instanceId\": \"e6bc03c6-c137-4221-b679-a7c5c31f986c\" , \"source\": "
        + "\"FOLIO\", \"updatedDate\": \"2020-06-15T11:07:48.563Z\",  \"deleted\": \"false\","
        + "\"suppressFromDiscovery\": \"false\"}\n\r { \"instanceId\"  \"a7bc91c6-c137-4221-"
        + "b679-a7c5c31f986c\", \"source\": \"FOLIO\",\"updatedDate\": \"2020-06-15T11:07:48."
        + "732Z\",  \"deleted\": \"false\" : \"suppressFromDiscovery\": \"false\"}";
    var events = new ArrayList<JsonEvent>();

    var jsonParser = new OaiPmhJsonParser()
        .objectValueMode()
        .handler(events::add)
        .exceptionHandler(handler -> log.error("Error parsing"));
    jsonParser.write(Buffer.buffer(json)).end();
    assertEquals(1, ((OaiPmhJsonParser) jsonParser).getErrors().size());
    assertEquals(2, events.size());
  }
}
