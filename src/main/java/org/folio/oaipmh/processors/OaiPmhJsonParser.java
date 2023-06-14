package org.folio.oaipmh.processors;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.parsetools.impl.JsonParserImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class OaiPmhJsonParser extends JsonParserImpl {

  private final Logger logger = LogManager.getLogger(OaiPmhJsonParser.class);

  private Handler<Throwable> oaiPmhExceptionalHandler;
  private final List<String> errors = new ArrayList<>();

  public OaiPmhJsonParser() {
    super(null);
  }

  @Override
  public JsonParser exceptionHandler(Handler<Throwable> handler) {
    this.oaiPmhExceptionalHandler = handler;
    return this;
  }

  @Override
  public void handle(Buffer data) {
    var normalized =  data.toString().replaceAll("([\\r\\n])", "");
    try {
      super.handle(Buffer.buffer(normalized));
    } catch (DecodeException e) {
      var errorResolver = new JsonParserErrorResolver(normalized, e.getLocalizedMessage());
      logger.error(e.getLocalizedMessage());
      logger.error("Error position at error part of json is {}", errorResolver.getErrorPosition());
      logger.error(errorResolver.getErrorPart());
      errors.add(errorResolver.getErrorPart());
      if (oaiPmhExceptionalHandler != null) {
        oaiPmhExceptionalHandler.handle(e);
        return;
      }
      throw e;
    }
  }

  @Override
  public void end() {
    try {
      super.end();
    } catch (DecodeException e) {
      if (oaiPmhExceptionalHandler != null) {
        oaiPmhExceptionalHandler.handle(e);
        return;
      }
      throw e;
    }
  }

  public List<String> getErrors() {
    return errors;
  }
}
