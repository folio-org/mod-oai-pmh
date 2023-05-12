package org.folio.oaipmh.processors;

import static java.util.Objects.nonNull;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.streams.WriteStream;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class JsonWriter implements WriteStream<Buffer> {

  private final JsonParser parser;
  private final AtomicInteger queueSize = new AtomicInteger(0);
  private final int loadBottomGreenLine;
  private final int maxQueueSize;
  private Handler<Void> drainHandler;
  private static final double BOTTOM_GREEN_LINE_MULTIPLIER = 1.15;
  private static final int MAX_QUEUE_SIZE_MULTIPLIER  = 3;
  public static final int CHUNK_SIZE_MULTIPLIER = 512;

  public JsonWriter(JsonParser parser, int chunkSize) {
    this.parser = parser;
    this.loadBottomGreenLine = (int) (chunkSize * CHUNK_SIZE_MULTIPLIER * BOTTOM_GREEN_LINE_MULTIPLIER);
    this.maxQueueSize = chunkSize * MAX_QUEUE_SIZE_MULTIPLIER * CHUNK_SIZE_MULTIPLIER;
  }

  @Override
  public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
    parser.exceptionHandler(handler);
    return this;
  }

  @Override
  public Future<Void> write(Buffer buffer) {
    Promise<Void> promise = Promise.promise();
    write(buffer, promise);
    return promise.future();
  }

  @Override
  public void write(Buffer data, Handler<AsyncResult<Void>> handler) {
    log.debug("Queue size before writing: " + queueSize.get());
    queueSize.addAndGet(data.getBytes().length);
    parser.handle(data);
    if (Objects.nonNull(handler)) {
      log.debug("Handler is working");
      handler.handle(Future.succeededFuture());
    }
    log.debug("Queue size after writing: " + queueSize.get());
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    parser.end();
    if (nonNull(handler)) {
      handler.handle(Future.succeededFuture());
    }
  }

  @Override
  public WriteStream<Buffer> setWriteQueueMaxSize(int maxQueueSize) {
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    var writeQueueFull = queueSize.get() >= maxQueueSize;
    log.debug("Queue is full: " + writeQueueFull);
    return writeQueueFull;
  }

  @Override
  public WriteStream<Buffer> drainHandler(Handler<Void> drainHandler) {
    this.drainHandler = drainHandler;
    return this;
  }

  public void chunkSent(int chunkSize) {
    log.debug("Chunk received, queue size: " + queueSize.get());
    if (queueSize.addAndGet(-chunkSize) <= loadBottomGreenLine) {
      var handler = drainHandler;
      if (nonNull(handler)) {
        log.debug("Drain handler is working");
        handler.handle(null);
      }
    } else {
      log.debug("Drain handler isn't executed");
    }
    log.debug("Chunk saved, queue size: " + queueSize.get());
  }
}
