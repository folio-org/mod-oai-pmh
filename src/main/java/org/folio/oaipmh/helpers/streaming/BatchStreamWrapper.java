package org.folio.oaipmh.helpers.streaming;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import com.sun.jdi.PrimitiveValue;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.core.streams.WriteStream;

/**
 * WriteStream wrapper to read from the stream in batches.
 */
public class BatchStreamWrapper implements WriteStream<JsonEvent> {

  private final Vertx vertx;

  private Handler<Void> drainHandler;
  private Handler<Throwable> exceptionHandler;
  private volatile Handler<List<JsonEvent>> batchReadyHandler;
  private volatile int batchSize;

  private volatile boolean streamEnded = false;

  private volatile boolean endedWithError = false;
  private volatile Throwable cause;

  private final List<JsonEvent> dataList = new CopyOnWriteArrayList<>();

  private final LongAdder returnedCount = new LongAdder();
  private final LongAdder page = new LongAdder();

  private Supplier<Boolean> capacityChecker;


  public BatchStreamWrapper(Vertx vertx, int batchSize) {
    this.vertx = vertx;
    this.batchSize = batchSize;
  }

  @Override
  public WriteStream<JsonEvent> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public WriteStream<JsonEvent> write(JsonEvent data) {
    return write(data, null);
  }

  @Override
  public synchronized WriteStream<JsonEvent> write(JsonEvent data,
                                                   Handler<AsyncResult<Void>> handler) {
    dataList.add(data);
    if (dataList.size() >= batchSize) {
      runBatchHandler();
    }
    return this;
  }

  private void runBatchHandler() {

      synchronized (BatchStreamWrapper.this) {
        if (batchReadyHandler != null) {
          int size = Math.min(dataList.size(), batchSize);
          ArrayList<JsonEvent> batch = new ArrayList<>(dataList.subList(0, size));
          page.increment();
          returnedCount.add(batch.size());
          dataList.subList(0, batch.size()).clear();
          batchReadyHandler.handle(batch);
          if (isTheLastBatch()) {
            batchReadyHandler = null;
          }
          invokeDrainHandler();
        }
      }
  }

  public synchronized void invokeDrainHandler() {
    if (drainHandler != null && !writeQueueFull()
    && !(streamEnded && dataList.isEmpty()) ) {
      drainHandler.handle(null);
    }
  }

  public boolean isTheLastBatch() {
    return (isStreamEnded() && dataList.isEmpty());
  }

  @Override
  public synchronized void end() {
    streamEnded = true;
    runBatchHandler();
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
    return dataList.size() > batchSize * 2 || capacityChecker.get();
  }

  @Override
  public WriteStream<JsonEvent> drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }


  public synchronized WriteStream<JsonEvent> handleBatch(Handler<List<JsonEvent>> handler) {
    batchReadyHandler = handler;
    if (dataList.size() >= batchSize || streamEnded) {
      runBatchHandler();
    }
    return this;
  }

  public void setCapacityChecker(Supplier<Boolean> supplier) {
    capacityChecker = supplier;
  }

  public boolean isStreamEnded() {
    return streamEnded;
  }

  public boolean isEndedWithError() {
    return endedWithError;
  }

  public Throwable getCause() {
    return cause;
  }

  public synchronized void endWithError(Throwable cause) {
    this.cause = cause;
    this.endedWithError = true;
    end();
  }

  public Long getReturnedCount() {
    return returnedCount.longValue();
  }

}
