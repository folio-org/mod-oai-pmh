package org.folio.oaipmh.helpers.streaming;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.core.streams.WriteStream;

//TODO JAVADOC
public class BatchStreamWrapper implements WriteStream<JsonEvent> {
  private final Vertx vertx;

  private Handler<Void> drainHandler;
  private Handler<Void> myHandler;
  private Handler<AsyncResult<Void>> endHandler;
  private volatile int batchSize = 10;

  private volatile boolean streamEnded = false;

  private final List<JsonEvent> dataList;


  public BatchStreamWrapper(Vertx vertx, List<JsonEvent> dataList) {
    this.vertx = vertx;
    this.dataList = dataList;
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
  public WriteStream<JsonEvent> write(JsonEvent data, Handler<AsyncResult<Void>> handler) {
    vertx.executeBlocking(promise -> {
      if (writeQueueFull()) {
        Handler<Void> o1 = myHandler;
        if (o1 != null) {
          o1.handle(null);
        }
        myHandler = null;
      }
      dataList.add(data);
      promise.complete();
    }, false, ar -> {
      if (handler != null) {
        handler.handle(ar.mapEmpty());
      }

      if ((endHandler != null)) {

        endHandler.handle(Future.succeededFuture());
      }
      if (!streamEnded) {

        drainHandler.handle(null);
      }
    });
    return this;
  }

  @Override
  public void end() {
    streamEnded = true;
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    streamEnded = true;
    if (handler != null) {
      endHandler = handler;
    }
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


  public WriteStream<JsonEvent> handleBatch(Handler<Void> handler) {
    if (writeQueueFull()) {
      handler.handle(null);
    } else {
      this.myHandler = handler;
    }
    return this;
  }

  public List<JsonEvent> getNext(int count) {
    final List<JsonEvent> batch = dataList.subList(0, count);
    //todo remove from list
    dataList.removeAll(batch);
    return batch;

  }
}
