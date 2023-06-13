package org.folio.oaipmh.dao.impl;

import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.dao.ErrorsDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.jooq.tables.pojos.Errors;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

import static java.util.stream.Collectors.toList;
import static org.folio.rest.jooq.tables.Errors.ERRORS;

@Repository
public class ErrorsDaoImpl implements ErrorsDao {

  protected final Logger logger = LogManager.getLogger(getClass());

  private PostgresClientFactory postgresClientFactory;

  public ErrorsDaoImpl(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<Errors> saveErrors(Errors errors, String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.execute(dslContext -> {
        var insertValues = dslContext.insertInto(ERRORS, ERRORS.REQUEST_ID,
          ERRORS.INSTANCE_FDD, ERRORS.ERROR_MSG);
        insertValues.values(errors.getRequestId(), errors.getInstanceFdd(), errors.getErrorMsg());
        return insertValues;
      })
      .map(rows -> null));
  }

  @Override
  public Future<List<Errors>> getErrorsList(String requestId, String tenantId) {
    return postgresClientFactory.getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor
      .query(dslContext -> dslContext.selectFrom(ERRORS)
        .where(ERRORS.REQUEST_ID.eq(UUID.fromString(requestId)))
        .orderBy(ERRORS.ID))
      .map(this::queryResultToErrorsList));
  }

  @Override
  public Future<Boolean> deleteErrorsByRequestId(String requestId, String tenantId) {
    logger.debug("Deleting errors with request id - {}.", requestId);
    return postgresClientFactory.getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor
      .execute(dslContext -> dslContext.deleteFrom(ERRORS)
        .where(ERRORS.REQUEST_ID.eq(UUID.fromString(requestId))))
      .map(res -> {
        if (res == 1) {
          logger.debug("Errors with request id '{}' have been deleted.", requestId);
          return true;
        }
        logger.info("No errors found to delete with request id {}.", requestId);
        return false;
      }));
  }

  private List<Errors> queryResultToErrorsList(QueryResult queryResult) {
    return queryResult.stream()
      .map(QueryResult::unwrap)
      .map(Row.class::cast)
      .map(row -> {
        Errors pojo = new Errors();
        pojo.setInstanceFdd(row.getString(ERRORS.INSTANCE_FDD.getName()));
        pojo.setRequestId(row.getUUID(ERRORS.REQUEST_ID.getName()));
        pojo.setErrorMsg(row.getString(ERRORS.ERROR_MSG.getName()));
        pojo.setId(row.getLong(ERRORS.ID.getName()));
        return pojo;
      })
      .collect(toList());
  }
}
