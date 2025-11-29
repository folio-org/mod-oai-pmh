package org.folio.oaipmh.processors;

import static java.lang.String.format;
import static java.util.Objects.nonNull;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.streams.WriteStream;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class JsonWriter implements WriteStream<Buffer> {
  private final JsonParser parser;
  private final AtomicInteger utilizationTracker;
  private final int trackerLimit;
  private Handler<Void> drainHandler;

  public JsonWriter(JsonParser parser, int trackerLimit) {
    this.parser = parser;
    this.trackerLimit = trackerLimit;
    this.utilizationTracker = new AtomicInteger(0);
  }

  @Override
  public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
    parser.exceptionHandler(handler);
    return this;
  }

  @Override
  public Future<Void> write(Buffer data) {
    parser.handle(data);
    return Future.succeededFuture();
  }

  @Override
  public Future<Void> end() {
    parser.end();
    return Future.succeededFuture();
  }

  @Override
  public WriteStream<Buffer> setWriteQueueMaxSize(int maxQueueSize) {
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return utilizationTracker.get() >= trackerLimit;
  }

  @Override
  public WriteStream<Buffer> drainHandler(Handler<Void> drainHandler) {
    this.drainHandler = drainHandler;
    return this;
  }

  public void chunkReceived() {
    var trackedValue = utilizationTracker.incrementAndGet();
    log.debug(format("Utilization tracker size: %s", trackedValue));
  }

  public void chunkSent() {
    var trackedValue = utilizationTracker.decrementAndGet();
    log.debug(format("Utilization tracker size: %s", trackedValue));
    if (trackedValue < trackerLimit) {
      var handler = drainHandler;
      if (nonNull(handler)) {
        handler.handle(null);
      }
    }
  }
}
