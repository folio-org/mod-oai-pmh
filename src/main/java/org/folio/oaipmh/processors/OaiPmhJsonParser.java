package org.folio.oaipmh.processors;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.parsetools.impl.JsonParserImpl;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OaiPmhJsonParser extends JsonParserImpl {

  private final Logger logger = LogManager.getLogger(OaiPmhJsonParser.class);

  private Handler<Throwable> oaiPmhExceptionalHandler;
  private final Set<String> errors = new HashSet<>();

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
      logger.error("Decode parser exception: Error position at error part of json is {}",
          errorResolver.getErrorPosition());
      logger.error(errorResolver.getErrorPart());
      errors.add(errorResolver.getErrorPart());
      if (handle(e)) {
        return;
      }
      throw e;
    } catch (Exception e) {
      logger.error("Generic parser exception: {}", e.getLocalizedMessage());
      errors.add(e.getLocalizedMessage());
      if (handle(e)) {
        return;
      }
      throw e;
    }
  }

  private boolean handle(Exception e) {
    if (oaiPmhExceptionalHandler != null) {
      oaiPmhExceptionalHandler.handle(e);
      return true;
    }
    return false;
  }

  @Override
  public void end() {
    try {
      super.end();
    } catch (Exception e) {
      if (oaiPmhExceptionalHandler != null) {
        oaiPmhExceptionalHandler.handle(e);
        return;
      }
      throw e;
    }
  }

  public List<String> getErrors() {
    return new ArrayList<>(errors);
  }
}
