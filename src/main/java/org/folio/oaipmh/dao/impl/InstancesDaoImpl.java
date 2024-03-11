package org.folio.oaipmh.dao.impl;

import static java.util.Objects.nonNull;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static org.folio.rest.jooq.tables.SuppressedFromDiscoveryInstancesIds.SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS;
import static org.folio.rest.jooq.tables.FailedInstancesIds.FAILED_INSTANCES_IDS;
import static org.folio.rest.jooq.tables.FailedToSaveInstancesIds.FAILED_TO_SAVE_INSTANCES_IDS;
import static org.folio.rest.jooq.tables.Instances.INSTANCES;
import static org.folio.rest.jooq.tables.RequestMetadataLb.REQUEST_METADATA_LB;
import static org.folio.rest.jooq.tables.SkippedInstancesIds.SKIPPED_INSTANCES_IDS;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import io.vertx.core.Promise;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.domain.StatisticsHolder;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.jaxrs.model.RequestMetadata;
import org.folio.rest.jaxrs.model.RequestMetadataCollection;
import org.folio.rest.jaxrs.model.UuidCollection;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.folio.rest.jooq.tables.records.FailedInstancesIdsRecord;
import org.folio.rest.jooq.tables.records.FailedToSaveInstancesIdsRecord;
import org.folio.rest.jooq.tables.records.InstancesRecord;
import org.folio.rest.jooq.tables.records.RequestMetadataLbRecord;
import org.folio.rest.jooq.tables.records.SkippedInstancesIdsRecord;
import org.folio.rest.jooq.tables.records.SuppressedFromDiscoveryInstancesIdsRecord;
import org.folio.s3.client.FolioS3Client;
import org.jooq.InsertValuesStep3;
import org.jooq.Record;
import org.springframework.beans.factory.annotation.Autowired;
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

  @Autowired
  private FolioS3Client folioS3Client;

  @Override
  public Future<List<String>> getExpiredRequestIds(String tenantId, long expirationPeriodInSeconds) {
    OffsetDateTime offsetDateTime = ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault())
      .minusSeconds(expirationPeriodInSeconds)
      .toOffsetDateTime();
    return getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor
      .query(dslContext -> dslContext.selectFrom(REQUEST_METADATA_LB)
        .where(REQUEST_METADATA_LB.LAST_UPDATED_DATE.lessOrEqual(offsetDateTime)))
      .map(this::mapRequestIdsResultToList));
  }

  @Override
  public Future<RequestMetadataLb> getRequestMetadataByRequestId(String requestId, String tenantId) {
    return getQueryExecutorReader(tenantId).transaction(queryExecutor ->
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
    if (nonNull(row)) {
      requestMetadataLb = RowMappers.getRequestMetadataLbMapper().apply(row);
    }
    return nonNull(requestMetadataLb) ? of(requestMetadataLb) : Optional.empty();
  }

  @Override
  public Future<RequestMetadataCollection> getRequestMetadataCollection(int offset, int limit, String tenantId) {
    return getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor.query(dslContext -> dslContext.selectCount().from(REQUEST_METADATA_LB))).compose(recordsCount ->
      getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor.query(dslContext -> dslContext.selectFrom(REQUEST_METADATA_LB)
          .orderBy(REQUEST_METADATA_LB.LAST_UPDATED_DATE.desc())
          .offset(offset)
          .limit(limit))
        .map(collection -> queryResultToRequestMetadataCollection(collection, recordsCount.get(0, int.class))))
    );
  }

  @Override
  public Future<UuidCollection> getFailedToSaveInstancesIdsCollection(String requestId, int offset, int limit, String tenantId) {
    return getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor.query(dslContext -> dslContext.selectCount().from(FAILED_TO_SAVE_INSTANCES_IDS)
            .where(FAILED_TO_SAVE_INSTANCES_IDS.REQUEST_ID.eq(UUID.fromString(requestId))))).compose(recordsCount ->
      getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor.query(dslContext -> dslContext.selectFrom(FAILED_TO_SAVE_INSTANCES_IDS)
          .where(FAILED_TO_SAVE_INSTANCES_IDS.REQUEST_ID.eq(UUID.fromString(requestId)))
          .offset(offset)
          .limit(limit))
        .map(collection -> queryResultToUuidCollection(collection, recordsCount.get(0, int.class))))
    );
  }

  @Override
  public Future<UuidCollection> getSkippedInstancesIdsCollection(String requestId, int offset, int limit, String tenantId) {
    return getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor.query(dslContext -> dslContext.selectCount().from(SKIPPED_INSTANCES_IDS)
            .where(SKIPPED_INSTANCES_IDS.REQUEST_ID.eq(UUID.fromString(requestId))))).compose(recordsCount ->
      getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor.query(dslContext -> dslContext.selectFrom(SKIPPED_INSTANCES_IDS)
          .where(SKIPPED_INSTANCES_IDS.REQUEST_ID.eq(UUID.fromString(requestId)))
          .offset(offset)
          .limit(limit))
        .map(collection -> queryResultToUuidCollection(collection, recordsCount.get(0, int.class))))
    );
  }

  @Override
  public Future<UuidCollection> getFailedInstancesIdsCollection(String requestId, int offset, int limit, String tenantId) {
    return getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor.query(dslContext -> dslContext.selectCount().from(FAILED_INSTANCES_IDS)
            .where(FAILED_INSTANCES_IDS.REQUEST_ID.eq(UUID.fromString(requestId))))).compose(recordsCount ->
      getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor.query(dslContext -> dslContext.selectFrom(FAILED_INSTANCES_IDS)
          .where(FAILED_INSTANCES_IDS.REQUEST_ID.eq(UUID.fromString(requestId)))
          .offset(offset)
          .limit(limit))
        .map(collection -> queryResultToUuidCollection(collection, recordsCount.get(0, int.class))))
    );
  }

  @Override
  public Future<UuidCollection> getSuppressedInstancesIdsCollection(String requestId, int offset, int limit, String tenantId) {
    return getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor.query(dslContext -> dslContext.selectCount().from(SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS)
            .where(SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS.REQUEST_ID.eq(UUID.fromString(requestId))))).compose(recordsCount ->
      getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor.query(dslContext -> dslContext.selectFrom(SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS)
          .where(SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS.REQUEST_ID.eq(UUID.fromString(requestId)))
          .offset(offset)
          .limit(limit))
        .map(collection -> queryResultToUuidCollection(collection, recordsCount.get(0, int.class))))
    );
  }

  @Override
  public Future<RequestMetadataLb> saveRequestMetadata(RequestMetadataLb requestMetadata, String tenantId) {
    UUID uuid = requestMetadata.getRequestId();
    requestMetadata.setStreamEnded(false);
    if (Objects.isNull(uuid) || StringUtils.isEmpty(uuid.toString())) {
      return Future
        .failedFuture(new IllegalStateException("Cannot save request metadata, request metadata entity must contain requestId"));
    }
    var startedDate = requestMetadata.getStartedDate();
    if (Objects.isNull(startedDate) || StringUtils.isEmpty(startedDate.toString())) {
      return Future
        .failedFuture(new IllegalStateException("Cannot save request metadata, request metadata entity must contain startedDate"));
    }
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .executeAny(dslContext -> dslContext.insertInto(REQUEST_METADATA_LB)
        .set(toDatabaseRecord(requestMetadata)))
      .map(raw -> requestMetadata));
  }

  @Override
  public Future<RequestMetadataLb> updateRequestUpdatedDateAndStatistics(String requestId, OffsetDateTime lastUpdatedDate, StatisticsHolder holder,
                                                                         String tenantId) {
    RequestMetadataLb requestMetadataLb = new RequestMetadataLb();
    requestMetadataLb.setRequestId(UUID.fromString(requestId))
      .setLastUpdatedDate(lastUpdatedDate);

    return getQueryExecutor(tenantId).transaction(queryExecutor -> {

      Promise<RequestMetadataLb> promise = Promise.promise();

      var updateRequestMetadataRecordFuture = queryExecutor
      .executeAny(dslContext -> dslContext.update(REQUEST_METADATA_LB)
        .set(REQUEST_METADATA_LB.LAST_UPDATED_DATE, lastUpdatedDate)
        .set(REQUEST_METADATA_LB.DOWNLOADED_AND_SAVED_INSTANCES_COUNTER, REQUEST_METADATA_LB.DOWNLOADED_AND_SAVED_INSTANCES_COUNTER.plus(holder.getDownloadedAndSavedInstancesCounter().get()))
        .set(REQUEST_METADATA_LB.FAILED_TO_SAVE_INSTANCES_COUNTER, REQUEST_METADATA_LB.FAILED_TO_SAVE_INSTANCES_COUNTER.plus(holder.getFailedToSaveInstancesCounter().get()))
        .set(REQUEST_METADATA_LB.RETURNED_INSTANCES_COUNTER, REQUEST_METADATA_LB.RETURNED_INSTANCES_COUNTER.plus(holder.getReturnedInstancesCounter().get()))
        .set(REQUEST_METADATA_LB.SKIPPED_INSTANCES_COUNTER, REQUEST_METADATA_LB.SKIPPED_INSTANCES_COUNTER.plus(holder.getSkippedInstancesCounter().get()))
        .set(REQUEST_METADATA_LB.FAILED_INSTANCES_COUNTER, REQUEST_METADATA_LB.FAILED_INSTANCES_COUNTER.plus(holder.getFailedInstancesCounter().get()))
        .set(REQUEST_METADATA_LB.SUPPRESSED_INSTANCES_COUNTER, REQUEST_METADATA_LB.SUPPRESSED_INSTANCES_COUNTER.plus(holder.getSuppressedFromDiscoveryCounter().get()))

        .where(REQUEST_METADATA_LB.REQUEST_ID.eq(UUID.fromString(requestId)))
        .returning())
      .map(this::toOptionalRequestMetadata)
      .map(optional -> {
        if (optional.isPresent()) {
          return optional.get();
        }
        throw new NotFoundException(String.format(REQUEST_METADATA_WITH_ID_DOES_NOT_EXIST, requestId));

      });

      var saveFailedToSaveInstancesIds = holder.getFailedToSaveInstancesIds().isEmpty() ? Future.succeededFuture() : queryExecutor.execute(dslContext -> {
          InsertValuesStep3<FailedToSaveInstancesIdsRecord, UUID, UUID, UUID> insertValues = dslContext.insertInto(FAILED_TO_SAVE_INSTANCES_IDS, FAILED_TO_SAVE_INSTANCES_IDS.ID, FAILED_TO_SAVE_INSTANCES_IDS.REQUEST_ID,
            FAILED_TO_SAVE_INSTANCES_IDS.INSTANCE_ID);
          holder.getFailedToSaveInstancesIds().forEach(id -> insertValues.values(UUID.randomUUID(), UUID.fromString(requestId), UUID.fromString(id)));
          return insertValues;
        })
        .map(rows -> null);

      var saveFailedInstancesIds = holder.getFailedInstancesIds().isEmpty() ? Future.succeededFuture() : queryExecutor.execute(dslContext -> {
          InsertValuesStep3<FailedInstancesIdsRecord, UUID, UUID, UUID> insertValues = dslContext.insertInto(FAILED_INSTANCES_IDS, FAILED_INSTANCES_IDS.ID, FAILED_INSTANCES_IDS.REQUEST_ID,
            FAILED_INSTANCES_IDS.INSTANCE_ID);
          holder.getFailedInstancesIds().forEach(id -> insertValues.values(UUID.randomUUID(), UUID.fromString(requestId), UUID.fromString(id)));
          return insertValues;
        })
        .map(rows -> null);

      var saveSkippedInstancesIds = holder.getSkippedInstancesIds().isEmpty() ? Future.succeededFuture() : queryExecutor.execute(dslContext -> {
          InsertValuesStep3<SkippedInstancesIdsRecord, UUID, UUID, UUID> insertValues = dslContext.insertInto(SKIPPED_INSTANCES_IDS, SKIPPED_INSTANCES_IDS.ID, SKIPPED_INSTANCES_IDS.REQUEST_ID,
            SKIPPED_INSTANCES_IDS.INSTANCE_ID);
          holder.getSkippedInstancesIds().forEach(id -> insertValues.values(UUID.randomUUID(), UUID.fromString(requestId), UUID.fromString(id)));
          return insertValues;
        })
        .map(rows -> null);

      var saveSuppressedFromDiscoveryInstancesIds = holder.getSuppressedInstancesIds().isEmpty() ? Future.succeededFuture() : queryExecutor.execute(dslContext -> {
          InsertValuesStep3<SuppressedFromDiscoveryInstancesIdsRecord, UUID, UUID, UUID> insertValues = dslContext.insertInto(SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS, SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS.ID, SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS.REQUEST_ID,
            SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS.INSTANCE_ID);
          holder.getSuppressedInstancesIds().forEach(id -> insertValues.values(UUID.randomUUID(), UUID.fromString(requestId), UUID.fromString(id)));
          return insertValues;
        })
        .map(rows -> null);


      GenericCompositeFuture.all(List.of(saveFailedToSaveInstancesIds, saveFailedInstancesIds, saveSkippedInstancesIds, saveSuppressedFromDiscoveryInstancesIds, updateRequestMetadataRecordFuture)).onComplete(x -> {
        if (x.succeeded()) {
          promise.complete(updateRequestMetadataRecordFuture.result());
        } else {
          promise.fail(x.cause());
        }
      });
      return promise.future();
    });
  }

  /**
   * @deprecated due to issue with VertX re-usage issue
   */
  @Deprecated(since = "3.7.2", forRemoval = true)
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
      return of(requestMetadataLb);
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
      .setStreamEnded(requestMetadata.getStreamEnded()).setLinkToErrorFile(requestMetadata.getLinkToErrorFile())
      .setStartedDate(requestMetadata.getStartedDate());
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
      InsertValuesStep3<InstancesRecord, UUID, UUID, Boolean> insertValues = dslContext.insertInto(INSTANCES, INSTANCES.INSTANCE_ID,
        INSTANCES.REQUEST_ID, INSTANCES.SUPPRESS_FROM_DISCOVERY);
      instances.forEach(instance -> insertValues.values(instance.getInstanceId(), instance.getRequestId(), instance.getSuppressFromDiscovery()));
      return insertValues;
    })
      .map(rows -> null));
  }

  @Override
  public Future<List<Instances>> getInstancesList(int limit, String requestId, String tenantId) {
    return getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor
      .query(dslContext -> dslContext.selectFrom(INSTANCES)
        .where(INSTANCES.REQUEST_ID.eq(UUID.fromString(requestId)))
        .orderBy(INSTANCES.ID)
        .limit(limit))
      .map(this::queryResultToInstancesList));
  }

  @Override
  public Future<RequestMetadataLb> updateRequestMetadataByPathToError(String requestId, String tenantId, String pathToErrorFile) {
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor
      .executeAny(dslContext ->
        dslContext.update(REQUEST_METADATA_LB)
          .set(REQUEST_METADATA_LB.PATH_TO_ERROR_FILE_IN_S3, pathToErrorFile)
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
  public Future<RequestMetadataLb> updateRequestMetadataByLinkToError(String requestId, String tenantId, String linkToError) {
    return getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor
      .executeAny(dslContext ->
        dslContext.update(REQUEST_METADATA_LB)
          .set(REQUEST_METADATA_LB.LINK_TO_ERROR_FILE, linkToError)
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
  public Future<List<String>> getRequestMetadataIdsByStartedDateAndExistsByPathToErrorFileInS3(String tenantId, OffsetDateTime date) {
    return getQueryExecutorReader(tenantId).transaction(queryExecutor -> queryExecutor
      .query(dslContext -> dslContext
        .selectFrom(REQUEST_METADATA_LB)
        .where(REQUEST_METADATA_LB.PATH_TO_ERROR_FILE_IN_S3.isNotNull())
        .and(REQUEST_METADATA_LB.PATH_TO_ERROR_FILE_IN_S3.ne(""))))
      .map(queryResult -> queryResultToRequestMetadataListWithProperStartedDate(queryResult, date));
  }

  private List<String> queryResultToRequestMetadataListWithProperStartedDate(QueryResult queryResult, OffsetDateTime date) {
    return queryResult.stream()
      .map(QueryResult::unwrap)
      .map(Row.class::cast)
      .filter(row -> row.getOffsetDateTime(REQUEST_METADATA_LB.STARTED_DATE.getName()).isBefore(date))
    .map(row -> row.getUUID(REQUEST_METADATA_LB.REQUEST_ID.getName()).toString())
      .collect(toList());
  }

  private List<Instances> queryResultToInstancesList(QueryResult queryResult) {
    return queryResult.stream()
      .map(QueryResult::unwrap)
      .map(Row.class::cast)
      .map(row -> {
        Instances pojo = new Instances();
        pojo.setInstanceId(row.getUUID(INSTANCES.INSTANCE_ID.getName()));
        pojo.setRequestId(row.getUUID(INSTANCES.REQUEST_ID.getName()));
        pojo.setSuppressFromDiscovery(row.getBoolean(INSTANCES.SUPPRESS_FROM_DISCOVERY.getName()));
        pojo.setId(row.getInteger(INSTANCES.ID.getName()));
        return pojo;
      })
      .collect(toList());
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

  private ReactiveClassicGenericQueryExecutor getQueryExecutorReader(String tenantId) {
    return postgresClientFactory.getQueryExecutorReader(tenantId);
  }

  private RequestMetadataCollection queryResultToRequestMetadataCollection(QueryResult queryResult, int totalRecordsCount) {
    List<RequestMetadata> list = queryResult.stream()
      .map(row -> rowToRequestMetadata(row.unwrap()))
      .collect(toList());
    return new RequestMetadataCollection().withRequestMetadataCollection(list)
      .withTotalRecords(totalRecordsCount);
  }

  private UuidCollection queryResultToUuidCollection(QueryResult queryResult, int totalRecordsCount) {
    List<String> list = queryResult.stream()
      .map(row -> rowToUUID(row.unwrap()).toString())
      .collect(toList());
    return new UuidCollection().withUuidCollection(list)
      .withTotalRecords(totalRecordsCount);
  }

  private RequestMetadata rowToRequestMetadata(Row row) {
    var pojo = RowMappers.getRequestMetadataLbMapper()
      .apply(row);
    var requestMetadata = new RequestMetadata();

    of(pojo.getRequestId()).ifPresent(uuid -> requestMetadata.withRequestId(uuid.toString()));
    of(pojo.getLastUpdatedDate())
      .ifPresent(offsetDateTime -> requestMetadata.withLastUpdatedDate(Date.from(offsetDateTime.toInstant())));
    of(pojo.getStreamEnded()).ifPresent(requestMetadata::withStreamEnded);
    of(pojo.getDownloadedAndSavedInstancesCounter()).ifPresent(requestMetadata::withDownloadedAndSavedInstancesCounter);
    of(pojo.getFailedToSaveInstancesCounter()).ifPresent(requestMetadata::withFailedToSaveInstancesCounter);
    of(pojo.getReturnedInstancesCounter()).ifPresent(requestMetadata::withReturnedInstancesCounter);
    of(pojo.getFailedInstancesCounter()).ifPresent(requestMetadata::withFailedInstancesCounter);
    of(pojo.getSkippedInstancesCounter()).ifPresent(requestMetadata::withSkippedInstancesCounter);
    of(pojo.getSuppressedInstancesCounter()).ifPresent(requestMetadata::withSuppressedInstancesCounter);
    ofNullable(pojo.getPathToErrorFileInS3()).ifPresentOrElse(pathToError -> {
      if (!pathToError.isEmpty()) {
        var regeneratedLink = folioS3Client.getPresignedUrl(pathToError);
        requestMetadata.withLinkToErrorFile(regeneratedLink);
      } else {
        requestMetadata.setLinkToErrorFile("");
      }
    }, () -> requestMetadata.setLinkToErrorFile(""));
    of(pojo.getStartedDate())
      .ifPresent(offsetDateTime -> requestMetadata.withStartedDate(Date.from(offsetDateTime.toInstant())));

    return requestMetadata;
  }

  private UUID rowToUUID(Row row) {
    return (UUID) row.getValue("instance_id");
  }

}
