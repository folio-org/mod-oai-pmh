package org.folio.oaipmh.dao.impl;

import static org.folio.rest.jooq.tables.Instances.INSTANCES;
import static org.folio.rest.jooq.tables.RequestMetadataLb.REQUEST_METADATA_LB;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.folio.rest.jooq.tables.records.InstancesRecord;
import org.folio.rest.jooq.tables.records.RequestMetadataLbRecord;
import org.jooq.InsertValuesStep3;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

@Repository
public class InstancesDaoImpl implements InstancesDao {

  protected final Logger logger = LogManager.getLogger(getClass());

  private static final String REQUEST_METADATA_WITH_ID_DOES_NOT_EXIST = "Request metadata with requestId - \"%s\" does not exists";

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
  public Future<RequestMetadataLb> getRequestMetadataByRequestId(String requestId, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor ->
      queryExecutor.findOneRow(dslContext ->
        dslContext.selectFrom(REQUEST_METADATA_LB)
      .where(REQUEST_METADATA_LB.REQUEST_ID.eq(UUID.fromString(requestId))))
        .map(this::toOptionalRequestMetadata)
    .map(optionalRequestMetadata -> {
      if (optionalRequestMetadata.isPresent()) {
        return optionalRequestMetadata.get();
      }
      throw new NotFoundException(String.format(REQUEST_METADATA_WITH_ID_DOES_NOT_EXIST, requestId));
    }));
  }

  private Optional<RequestMetadataLb> toOptionalRequestMetadata(Row row) {
    RequestMetadataLb requestMetadataLb = null;
    if (Objects.nonNull(row)) {
      requestMetadataLb = RowMappers.getRequestMetadataLbMapper().apply(row);
    }
    return Objects.nonNull(requestMetadataLb) ? Optional.of(requestMetadataLb) : Optional.empty();
  }

  @Override
  public Future<RequestMetadataLb> saveRequestMetadata(RequestMetadataLb requestMetadata, String tenantId) {
    UUID uuid = requestMetadata.getRequestId();
    requestMetadata.setStreamEnded(false);
    if (Objects.isNull(uuid) || StringUtils.isEmpty(uuid.toString())) {
      return Future
        .failedFuture(new IllegalStateException("Cannot save request metadata, request metadata entity must contain requestId"));
    }
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .executeAny(dslContext -> dslContext.insertInto(REQUEST_METADATA_LB)
        .set(toDatabaseRecord(requestMetadata)))
      .map(raw -> requestMetadata));
  }

  @Override
  public Future<RequestMetadataLb> updateRequestUpdatedDate(String requestId, OffsetDateTime lastUpdatedDate,
      String tenantId) {
    RequestMetadataLb requestMetadataLb = new RequestMetadataLb();
    requestMetadataLb.setRequestId(UUID.fromString(requestId))
      .setLastUpdatedDate(lastUpdatedDate);

    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .executeAny(dslContext -> dslContext.update(REQUEST_METADATA_LB)
        .set(REQUEST_METADATA_LB.LAST_UPDATED_DATE, lastUpdatedDate)
        .where(REQUEST_METADATA_LB.REQUEST_ID.eq(UUID.fromString(requestId)))
        .returning())
      .map(this::toOptionalRequestMetadata)
      .map(optional -> {
        if (optional.isPresent()) {
          return optional.get();
        }
        throw new NotFoundException(String.format(REQUEST_METADATA_WITH_ID_DOES_NOT_EXIST, requestId));
      }));
  }

  @Override
  public Future<RequestMetadataLb> updateRequestStreamEnded(String requestId, boolean isStreamEnded, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .executeAny(dslContext -> dslContext.update(REQUEST_METADATA_LB)
        .set(REQUEST_METADATA_LB.STREAM_ENDED, isStreamEnded)
        .where(REQUEST_METADATA_LB.REQUEST_ID.eq(UUID.fromString(requestId)))
        .returning())
      .map(this::toOptionalRequestMetadata)
      .map(optional -> {
        if (optional.isPresent()) {
          return optional.get();
        }
        throw new NotFoundException(String.format(REQUEST_METADATA_WITH_ID_DOES_NOT_EXIST, requestId));
      }));
  }

  private Optional<RequestMetadataLb> toOptionalRequestMetadata(RowSet<Row> rows) {
    if (rows.rowCount() == 1) {
      Row row = rows.iterator()
        .next();
      RequestMetadataLb requestMetadataLb = RowMappers.getRequestMetadataLbMapper()
        .apply(row);
      return Optional.of(requestMetadataLb);
    }
    return Optional.empty();
  }

  @Override
  public Future<Boolean> deleteRequestMetadataByRequestId(String requestId, String tenantId) {
    logger.debug("Deleting request metadata with request id - {}.", requestId);
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .execute(dslContext -> dslContext.deleteFrom(REQUEST_METADATA_LB)
        .where(REQUEST_METADATA_LB.REQUEST_ID.eq(UUID.fromString(requestId))))
      .map(res -> {
        if (res == 1) {
          logger.debug("Request metadata with id '{}' has been deleted.", requestId);
          return true;
        }
        logger.error("Cannot delete request metadata with id {}.", requestId);
        throw new NotFoundException(String.format(REQUEST_METADATA_WITH_ID_DOES_NOT_EXIST, requestId));
      }));
  }

  private Record toDatabaseRecord(RequestMetadataLb requestMetadata) {
    return new RequestMetadataLbRecord().setRequestId(requestMetadata.getRequestId())
      .setLastUpdatedDate(requestMetadata.getLastUpdatedDate())
      .setStreamEnded(requestMetadata.getStreamEnded());
  }

  @Override
  public Future<Boolean> deleteInstancesById(List<String> instIds, String requestId, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .execute(dslContext -> dslContext.deleteFrom(INSTANCES)
        .where(INSTANCES.INSTANCE_ID.in(instIds))
        .and(INSTANCES.REQUEST_ID.eq(UUID.fromString(requestId))))
      .map(res -> {
        String instanceIds = String.join(",", instIds);
        if (res > 0) {
          logger.debug("Instances with ids [{}] have been removed.", instanceIds);
          return true;
        } else {
          logger.debug("Cannot delete instances: there no instances with id's - [{}].", instanceIds);
          return false;
        }
      }));
  }

  @Override
  public Future<Void> saveInstances(List<Instances> instances, String tenantId) {
    if (instances.isEmpty()) {
      logger.debug("Skip saving instances. Instances list is empty.");
      return Future.succeededFuture();
    }
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.execute(dslContext -> {
      InsertValuesStep3<InstancesRecord, UUID, String, UUID> insertValues = dslContext.insertInto(INSTANCES, INSTANCES.INSTANCE_ID,
          INSTANCES.JSON, INSTANCES.REQUEST_ID);
      instances.forEach(instance -> insertValues.values(instance.getInstanceId(), instance.getJson(), instance.getRequestId()));
      return insertValues;
    })
      .map(rows -> null));
  }

  @Override
  public Future<List<Instances>> getInstancesList(int limit, String requestId, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .query(dslContext -> dslContext.selectFrom(INSTANCES)
        .where(INSTANCES.REQUEST_ID.eq(UUID.fromString(requestId)))
        .orderBy(INSTANCES.ID)
        .limit(limit))
      .map(this::queryResultToInstancesList));
  }

  @Override
  public Future<List<Instances>> getInstancesList(int limit, String requestId, int id, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .query(dslContext -> dslContext.selectFrom(INSTANCES)
        .where(INSTANCES.REQUEST_ID.eq(UUID.fromString(requestId)))
        .and(INSTANCES.ID.greaterOrEqual(id))
        .orderBy(INSTANCES.ID)
        .limit(limit))
      .map(this::queryResultToInstancesList));
  }

  private List<Instances> queryResultToInstancesList(QueryResult queryResult) {
    return queryResult.stream()
      .map(QueryResult::unwrap)
      .map(Row.class::cast)
      .map(row -> {
        Instances pojo = new Instances();
        pojo.setInstanceId(row.getUUID(INSTANCES.INSTANCE_ID.getName()));
        pojo.setJson(row.getString(INSTANCES.JSON.getName()));
        pojo.setRequestId(row.getUUID(INSTANCES.REQUEST_ID.getName()));
        pojo.setId(row.getInteger(INSTANCES.ID.getName()));
        return pojo;
      })
      .collect(Collectors.toList());
  }

  private List<String> mapRequestIdsResultToList(QueryResult requestIds) {
    List<String> ids = requestIds.stream()
      .map(row -> {
        RequestMetadataLb pojo = RowMappers.getRequestMetadataLbMapper()
          .apply(row.unwrap());
        return pojo.getRequestId()
          .toString();
      })
      .collect(Collectors.toList());
    var idList = String.join(",", ids);
    logger.debug("Expired request ids result: [{}].", idList);
    return ids;
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }

}
