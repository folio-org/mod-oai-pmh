package org.folio.oaipmh.helpers.streaming;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.core.streams.WriteStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * WriteStream wrapper to read from the stream in batches.
 */
public class BatchStreamWrapper implements WriteStream<JsonEvent> {

  private final Vertx vertx;

  private Handler<Void> drainHandler;
  private Handler<List<JsonEvent>> batchReadyHandler;
  private volatile int batchSize;

  private volatile boolean streamEnded = false;

  private final List<JsonEvent> dataList = new CopyOnWriteArrayList<>();


  public BatchStreamWrapper(Vertx vertx, int batchSize) {
    this.vertx = vertx;
    this.batchSize = batchSize;
  }

  @Override
  public WriteStream<JsonEvent> exceptionHandler(Handler<Throwable> handler) {
    return this;
  }

  @Override
  public WriteStream<JsonEvent> write(JsonEvent data) {
    return write(data, null);
  }

  @Override
  public synchronized WriteStream<JsonEvent> write(JsonEvent data,
    Handler<AsyncResult<Void>> handler) {
    if (writeQueueFull()) {
      runBatchHandler();
    }
    dataList.add(data);
    return this;
  }

  private void runBatchHandler() {
    vertx.executeBlocking(p -> {
      synchronized (BatchStreamWrapper.this) {
        if (batchReadyHandler != null) {
          batchReadyHandler.handle(dataList);
        }
        batchReadyHandler = null;
        dataList.clear();
        p.complete();
      }
    }, e -> {
    });
  }

  @Override
  public void end() {
    runBatchHandler();
    streamEnded = true;
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    end();
    handler.handle(null);
  }

  @Override
  public WriteStream<JsonEvent> setWriteQueueMaxSize(int maxSize) {
    batchSize = maxSize;
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return dataList.size() == batchSize;
  }

  @Override
  public WriteStream<JsonEvent> drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }


  public synchronized WriteStream<JsonEvent> handleBatch(Handler<List<JsonEvent>> handler) {
    batchReadyHandler = handler;
    if (writeQueueFull()) {
      runBatchHandler();
    }
    return this;
  }

  public boolean isStreamEnded() {
    return streamEnded;
  }
}
