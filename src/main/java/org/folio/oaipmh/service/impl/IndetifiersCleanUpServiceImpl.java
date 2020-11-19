package org.folio.oaipmh.service.impl;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.oaipmh.service.IndetifiersCleanUpService;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.SQLConnection;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class IndetifiersCleanUpServiceImpl implements IndetifiersCleanUpService {

  private static final long EXPIRATION_TIME_IN_MILLS = 7200_000;
  private static final String REQUEST_METADATA_TABLE_NAME = "REQUEST_METADATA";
  private static final String LAST_UPDATE_DAE_COLUMN_NAME = "LAST_UPDATE_DATE";
  private static final String REQUEST_ID_COLUMN_NAME = "REQUEST_ID";
  private static final String INSTANCES_TABLE_NAME = "INSTANCES";

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public Future<Boolean> cleanUp(OkapiConnectionParams params, Context vertxContext) {
    Promise<Boolean> promise = Promise.promise();
    PostgresClient postgresClient = PostgresClient.getInstance(vertxContext.owner(), params.getTenantId());
    final String getExpiredRequestsMetadataSql = format("SELECT json FROM " + REQUEST_METADATA_TABLE_NAME + " WHERE " +
      LAST_UPDATE_DAE_COLUMN_NAME + " <= '%s'",  new Date(new Date().getTime() - EXPIRATION_TIME_IN_MILLS));
    postgresClient.startTx(conn -> {
      if (conn.failed()) {
        logger.error("Cannot get connection for clean ids: " + conn.cause().getMessage(), conn.cause());
      } else {
        try {
          postgresClient.select(conn, getExpiredRequestsMetadataSql, reply -> {
            if (reply.succeeded()) {
              logger.info("Expired requests metadata: " + reply.result());
              StreamSupport.stream(reply.result().spliterator(), false)
                .map(record -> deleteByRequestId(record.getString(REQUEST_ID_COLUMN_NAME), postgresClient, conn));
              endTransaction(postgresClient, conn).future().onComplete(o -> promise.complete());
            } else {
              logger.error("Selecting instances failed: " + reply.cause());
              endTransaction(postgresClient, conn).future().onComplete(o -> handleException(promise, reply.cause()));
            }
          });
        } catch (Exception e) {
          endTransaction(postgresClient, conn).future().onComplete(o -> handleException(promise, e));
        }
      }
    });
    return promise.future();
  }

  private Future<Void> deleteByRequestId(String requestId, PostgresClient postgresClient, AsyncResult<SQLConnection> conn) {
    Promise<Void> promise = Promise.promise();
    final String deleteByRequestIdSql = format("DELETE FROM " + INSTANCES_TABLE_NAME + " WHERE " +
      REQUEST_ID_COLUMN_NAME + " = '%s'", requestId);
    try {
      postgresClient.execute(conn, deleteByRequestIdSql, reply -> {
        if (reply.failed()) {
          endTransaction(postgresClient, conn).future().onComplete(o -> handleException(promise, reply.cause()));
        }
      });
    } catch (Exception e) {
      endTransaction(postgresClient, conn).future().onComplete(o -> handleException(promise, e));
    }
    return promise.future();
  }

  private Promise<Void> endTransaction(PostgresClient postgresClient, AsyncResult<SQLConnection> conn) {
    Promise<Void> promise = Promise.promise();
    try {
      postgresClient.endTx(conn, promise);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      handleException(promise, e);
    }
    return promise;
  }

  private void handleException(Promise<?> promise, Throwable e) {
    logger.error(e.getMessage(), e);
    promise.fail(e);
  }
}
