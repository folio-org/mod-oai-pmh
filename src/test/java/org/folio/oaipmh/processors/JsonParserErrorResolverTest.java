package org.folio.oaipmh.processors;

import io.vertx.core.buffer.Buffer;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.service.ErrorService;
import org.folio.oaipmh.service.impl.ErrorServiceImpl;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonParserErrorResolverTest {

  @ParameterizedTest
  @ValueSource(strings = {"instanceId", "some_field"})
  void testGetErrorPartAndErrorPosition(String instanceIdField) {
    var json = "{\"" + instanceIdField + "\":\"e54b1f4d-7d05-4b1a-9368-3c36b75d8ac6\",\"source\":\"FOLIO\"," +
      "\"holdings\":[{\"id\":\"e9285a1c-1dfc-4380-868c-e74073003f43\",\"notes\":[]," +
      "\"location\":{\"effectiveLocation\":{\"code\" \"KU/CC/DI/M\",\"name\":\"Main Library\"," +
      "\"campusName\":\"City Campus\"},\"permanentLocation\":{\"code\":\"KU/CC/DI/M\",\"name\":\"Main Library\"," +
      "\"campusName\":\"City Campus\",\"libraryName\":\"Datalogisk Institut\"}}" +
      ",\"formerIds\":[],\"callNumber\":{\"callNumber\":\"M1366.S67 T73 2017\"}," +
      "\"suppressFromDiscovery\":false, \"holdingsStatementsForSupplements\":[]}]}";

    int errorPositionByParser = 180;
    var errors = new ArrayList<String>();

    ErrorService errorService = new ErrorServiceImpl();
    Request request = Request.builder().build();

    var jsonParser = new OaiPmhJsonParser(errorService, request)
      .objectValueMode().exceptionHandler(e -> errors.add(e.getLocalizedMessage()));
    jsonParser.write(Buffer.buffer(json)).end();

    var jsonParserErrorResolver = new JsonParserErrorResolver(json, errors.get(0));

    var expected = json.substring(errorPositionByParser, errorPositionByParser + 20);
    int errorPositionByParserErrorResolver = jsonParserErrorResolver.getErrorPosition();
    var actual = jsonParserErrorResolver.getErrorPart().substring(errorPositionByParserErrorResolver, errorPositionByParserErrorResolver + 20);

    assertEquals(expected, actual);
  }
}
