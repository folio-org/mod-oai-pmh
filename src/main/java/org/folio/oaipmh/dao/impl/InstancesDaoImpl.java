package org.folio.oaipmh.dao.impl;

import static org.folio.rest.jooq.tables.Instances.INSTANCES;
import static org.folio.rest.jooq.tables.RequestMetadataLb.REQUEST_METADATA_LB;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.folio.rest.jooq.tables.records.InstancesRecord;
import org.folio.rest.jooq.tables.records.RequestMetadataLbRecord;
import org.jooq.InsertValuesStep3;
import org.jooq.JSON;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Row;

@Repository
public class InstancesDaoImpl implements InstancesDao {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  private PostgresClientFactory postgresClientFactory;

  public InstancesDaoImpl(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
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

  @Override
  public Future<RequestMetadataLb> saveRequestMetadata(RequestMetadataLb requestMetadata, String tenantId) {
    requestMetadata.setId(UUID.randomUUID());
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .executeAny(dslContext -> dslContext.insertInto(REQUEST_METADATA_LB)
        .set(toDatabaseRecord(requestMetadata)))
      .map(raw -> requestMetadata));
  }

  @Override
  public Future<Boolean> deleteRequestMetadataByRequestId(String requestId, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .execute(dslContext -> dslContext.deleteFrom(REQUEST_METADATA_LB)
        .where(REQUEST_METADATA_LB.REQUEST_ID.eq(requestId)))
      .map(res -> {
        if (res == 1) {
          return true;
        }
        throw new NotFoundException(String.format("Request metadata with requestId - \"%s\" does not exists", requestId));
      }));
  }

  private Record toDatabaseRecord(RequestMetadataLb requestMetadata) {
    return new RequestMetadataLbRecord().setId(requestMetadata.getId())
      .setRequestId(requestMetadata.getRequestId())
      .setLastUpdatedDate(requestMetadata.getLastUpdatedDate());
  }

  @Override
  public Future<Boolean> deleteExpiredInstancesByRequestId(String tenantId, List<String> requestIds) {
    logger.debug("Deleting instances associated with requestIds[{}]", String.join(",", requestIds));
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .execute(dslContext -> dslContext.deleteFrom(INSTANCES)
        .where(INSTANCES.REQUEST_ID.in(requestIds)))
      .map(res -> {
        String requestIdsString = String.join(",", requestIds);
        if (res == 1) {
          logger.debug("Instances associated with requestIds[{}] have been removed.", requestIdsString);
          return true;
        } else {
          logger.debug("Cannot delete instances: there no any instances associated with requestIds[{}]", requestIdsString);
          return false;
        }
      }));
  }

  // possible problem -> before passing list of string convert it to list of UUID's
  @Override
  public Future<Boolean> deleteInstancesById(List<String> instIds, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .execute(dslContext -> dslContext.deleteFrom(INSTANCES)
        .where(INSTANCES.INSTANCE_ID.in(instIds)))
      .map(res -> {
        String instanceIds = String.join(",", instIds);
        if (res == 1) {
          logger.debug("Instances with ids [{}] have been removed.", instanceIds);
          return true;
        } else {
          logger.debug("Cannot delete instances: there no any instances with id's - [{}]", instanceIds);
          return false;
        }
      }));
  }

  private List<String> toList(QueryResult queryResult) {
    QueryResult queryResult1 = queryResult.unwrap();
    return Collections.emptyList();
  }

  @Override
  public Future<Void> saveInstances(List<Instances> instances, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.execute(dslContext -> {
      InsertValuesStep3<InstancesRecord, UUID, JSON, String> insertValues = dslContext.insertInto(INSTANCES, INSTANCES.INSTANCE_ID,
          INSTANCES.JSON, INSTANCES.REQUEST_ID);
      instances.forEach(instance -> insertValues.values(instance.getInstanceId(), instance.getJson(), instance.getRequestId()));
      return insertValues;
    })
      .map(rows -> null));
  }

  private List<InstancesRecord> listToRecords(List<Instances> instances) {
    return instances.stream()
      .map(instance -> new InstancesRecord().setInstanceId(instance.getInstanceId())
        .setRequestId(instance.getRequestId())
        .setJson(instance.getJson()))
      .collect(Collectors.toList());
  }

  @Override
  public Future<List<Instances>> getInstancesList(int offset, int limit, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .query(dslContext -> dslContext.selectFrom(INSTANCES)
        .offset(offset)
        .limit(limit))
      .map(this::queryResultToInstancesList));
  }

  // inline variable
  private List<Instances> queryResultToInstancesList(QueryResult queryResult) {
    List<Instances> instances = queryResult.stream()
      .map(QueryResult::unwrap)
      .map(Row.class::cast)
      .map(row -> {
        Instances pojo = new Instances();
        pojo.setInstanceId(row.getUUID(INSTANCES.INSTANCE_ID.getName()));
        pojo.setJson(JSON.valueOf(row.getBuffer(INSTANCES.JSON.getName())
          .toString()));
        pojo.setRequestId(row.getString(INSTANCES.REQUEST_ID.getName()));
        return pojo;
      })
      .collect(Collectors.toList());
    return instances;
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
