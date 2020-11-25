package org.folio.oaipmh.dao.impl;

import static org.folio.rest.jooq.tables.Instances.INSTANCES;
import static org.folio.rest.jooq.tables.RequestMetadataLb.REQUEST_METADATA_LB;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;

import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.springframework.stereotype.Repository;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@Repository
public class InstancesDaoImpl implements InstancesDao {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  private PostgresClientFactory postgresClientFactory;

  public InstancesDaoImpl(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<Boolean> deleteExpiredInstancesByRequestId(String tenantId, List<String> requestIds) {
    logger.debug("Deleting instances associated with requestIds[{0}]", String.join(",", requestIds));
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .execute(dslContext -> dslContext.deleteFrom(INSTANCES)
        .where(INSTANCES.REQUEST_ID.in(requestIds)))
      .map(res -> {
        String requestIdsString = String.join(",", requestIds);
        if (res == 1) {
          logger.debug("Instances associated with requestIds[{0}] have been removed.", requestIdsString);
          return true;
        } else {
          logger.debug("Cannot delete instances: there no any instances associated with requestIds[{0}]", requestIdsString);
          return false;
        }
      }));
  }

  @Override
  public Future<List<String>> getExpiredRequestIds(String tenantId, int expirationPeriodInSeconds) {
    OffsetDateTime offsetDateTime = ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault())
      .minusSeconds(expirationPeriodInSeconds)
      .toOffsetDateTime();
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .query(dslContext -> dslContext.selectFrom(REQUEST_METADATA_LB)
        .where(REQUEST_METADATA_LB.LAST_UPDATED_DATE.lessOrEqual(offsetDateTime)))
      .map(this::mapRequestIdsResultToList));
  }

  private List<String> mapRequestIdsResultToList(QueryResult requestIds) {
    List<String> ids = requestIds.stream()
      .map(row -> {
        RequestMetadataLb pojo = RowMappers.getRequestMetadataLbMapper()
          .apply(row.unwrap());
        return pojo.getRequestId();
      })
      .collect(Collectors.toList());
    logger.debug("Expired request ids result: " + String.join(",", ids));
    return ids;
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }

}
