package org.folio.oaipmh.dao.impl;

import static java.util.stream.Collectors.toList;
import static org.folio.rest.jooq.tables.Errors.ERRORS;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.dao.ErrorsDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.jooq.tables.pojos.Errors;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

@Repository
public class ErrorsDaoImpl implements ErrorsDao {

  private static final Logger logger = LogManager.getLogger(ErrorsDaoImpl.class);

  private static final DSLContext JOOQ = DSL.using(SQLDialect.POSTGRES);

  private final PostgresClientFactory postgresClientFactory;

  public ErrorsDaoImpl(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<Errors> saveErrors(Errors errors, String tenantId) {

    var query = JOOQ.insertInto(ERRORS)
        .columns(ERRORS.REQUEST_ID, ERRORS.INSTANCE_ID, ERRORS.ERROR_MSG)
        .values(errors.getRequestId(), errors.getInstanceId(), errors.getErrorMsg())
        .onConflict()
        .doNothing();
    var sql = query.getSQL(ParamType.INLINED);
    var client = postgresClientFactory.getPoolWriter(tenantId);

    return client
        .query(sql)
        .execute()
        .map(rows -> null);
  }

  @Override
  public Future<List<Errors>> getErrorsList(String requestId, String tenantId) {
    var uuid = UUID.fromString(requestId);

    var query = JOOQ.selectFrom(ERRORS)
        .where(ERRORS.REQUEST_ID.eq(uuid))
        .orderBy(ERRORS.ID);
    var sql = query.getSQL(ParamType.INLINED);
    var client = postgresClientFactory.getPoolReader(tenantId);

    return client
        .query(sql)
        .execute()
        .map(this::rowSetToErrorsList);
  }

  @Override
  public Future<Boolean> deleteErrorsByRequestId(String requestId, String tenantId) {

    var uuid = UUID.fromString(requestId);
    var query = JOOQ.deleteFrom(ERRORS)
        .where(ERRORS.REQUEST_ID.eq(uuid));
    var sql = query.getSQL(ParamType.INLINED);
    var client = postgresClientFactory.getPoolWriter(tenantId);

    return client
        .query(sql)
        .execute()
        .map(result -> {
          int deleted = result.rowCount();
          if (deleted > 0) {
            logger.debug("Errors with request id '{}' have been deleted. Rows: {}",
                requestId, deleted);
            return true;
          }
          logger.info("No errors found to delete with request id {}.", requestId);
          return false;
        });
  }

  private List<Errors> rowSetToErrorsList(RowSet<Row> rows) {
    return rows.stream()
        .map(this::rowToErrors)
        .collect(toList());
  }

  private Errors rowToErrors(Row row) {
    Errors errors = new Errors();
    errors.setId(row.getLong(ERRORS.ID.getName()));
    errors.setRequestId(row.getUUID(ERRORS.REQUEST_ID.getName()));
    errors.setInstanceId(row.getString(ERRORS.INSTANCE_ID.getName()));
    errors.setErrorMsg(row.getString(ERRORS.ERROR_MSG.getName()));
    return errors;
  }
}
