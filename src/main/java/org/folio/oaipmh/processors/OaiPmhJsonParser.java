package org.folio.oaipmh.processors;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.parsetools.impl.JsonParserImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OaiPmhJsonParser extends JsonParserImpl {

  private final Logger logger = LogManager.getLogger(OaiPmhJsonParser.class);

  private Handler<Throwable> oaiPmhExceptionalHandler;

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
}
