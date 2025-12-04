package org.folio.oaipmh.dao.impl;

import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static org.folio.rest.jooq.tables.FailedInstancesIds.FAILED_INSTANCES_IDS;
import static org.folio.rest.jooq.tables.FailedToSaveInstancesIds.FAILED_TO_SAVE_INSTANCES_IDS;
import static org.folio.rest.jooq.tables.Instances.INSTANCES;
import static org.folio.rest.jooq.tables.RequestMetadataLb.REQUEST_METADATA_LB;
import static org.folio.rest.jooq.tables.SkippedInstancesIds.SKIPPED_INSTANCES_IDS;
import static org.folio.rest.jooq.tables.SuppressedFromDiscoveryInstancesIds.SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS;

import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.ws.rs.NotFoundException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.domain.StatisticsHolder;
import org.folio.rest.jaxrs.model.RequestMetadata;
import org.folio.rest.jaxrs.model.RequestMetadataCollection;
import org.folio.rest.jaxrs.model.UuidCollection;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

@Repository
public class InstancesDaoImpl implements InstancesDao {

  private static final Logger logger = LogManager.getLogger(InstancesDaoImpl.class);

  private static final String REQUEST_METADATA_WITH_ID_DOES_NOT_EXIST =
      "Request metadata with requestId - \"%s\" does not exists";

  private static final String C_REQUEST_ID = "request_id";
  private static final String C_INSTANCE_ID = "instance_id";
  private static final String C_ID = "id";

  private static final String C_LAST_UPDATED_DATE = "last_updated_date";
  private static final String C_STARTED_DATE = "started_date";
  private static final String C_STREAM_ENDED = "stream_ended";
  private static final String C_LINK_TO_ERROR_FILE = "link_to_error_file";
  private static final String C_PATH_TO_ERROR_FILE_IN_S3 = "path_to_error_file_in_s3";
  private static final String C_DOWNLOADED_AND_SAVED = "downloaded_and_saved_instances_counter";
  private static final String C_FAILED_TO_SAVE = "failed_to_save_instances_counter";
  private static final String C_RETURNED = "returned_instances_counter";
  private static final String C_SKIPPED = "skipped_instances_counter";
  private static final String C_FAILED = "failed_instances_counter";
  private static final String C_SUPPRESSED = "suppressed_instances_counter";

  private static final String C_SUPPRESS_FROM_DISCOVERY = "suppress_from_discovery";

  private static final DSLContext JOOQ = DSL.using(SQLDialect.POSTGRES);

  private final PostgresClientFactory postgresClientFactory;

  public InstancesDaoImpl(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<List<String>> getExpiredRequestIds(String tenantId,
      long expirationPeriodInSeconds) {

    var offsetDateTime = ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault())
        .minusSeconds(expirationPeriodInSeconds)
        .toOffsetDateTime();

    var query = JOOQ
        .select(REQUEST_METADATA_LB.REQUEST_ID)
        .from(REQUEST_METADATA_LB)
        .where(REQUEST_METADATA_LB.LAST_UPDATED_DATE.lessOrEqual(offsetDateTime));

    return execute(reader(tenantId), query)
        .map(this::mapRequestIdsRowSetToList);
  }

  @Override
  public Future<RequestMetadataLb> getRequestMetadataByRequestId(String requestId,
      String tenantId) {

    var uuid = UUID.fromString(requestId);

    var query = JOOQ
        .selectFrom(REQUEST_METADATA_LB)
        .where(REQUEST_METADATA_LB.REQUEST_ID.eq(uuid))
        .limit(1);

    return execute(reader(tenantId), query)
        .map(rows -> singleRequestMetadataLbOrThrow(rows, requestId));
  }

  @Override
  public Future<RequestMetadataCollection> getRequestMetadataCollection(int offset,
      int limit, String tenantId) {

    var count = JOOQ
        .selectCount()
        .from(REQUEST_METADATA_LB);

    var data = JOOQ
        .selectFrom(REQUEST_METADATA_LB)
        .orderBy(REQUEST_METADATA_LB.LAST_UPDATED_DATE.desc())
        .offset(offset)
        .limit(limit);

    var client = reader(tenantId);

    return execute(client, count)
        .compose(countRs -> {
          int total = countRs.iterator().next().getInteger(0);
          return execute(client, data)
              .map(results -> {
                List<RequestMetadata> list = new ArrayList<>();
                for (Row row : results) {
                  list.add(rowToRequestMetadata(row));
                }
                return new RequestMetadataCollection()
                    .withRequestMetadataCollection(list)
                    .withTotalRecords(total);
              });
        });
  }

  @Override
  public Future<UuidCollection> getFailedToSaveInstancesIdsCollection(String requestId,
      int offset, int limit, String tenantId) {

    return getUuidCollectionForTable(
        FAILED_TO_SAVE_INSTANCES_IDS.REQUEST_ID,
        FAILED_TO_SAVE_INSTANCES_IDS.INSTANCE_ID,
        requestId, offset, limit, tenantId);
  }

  @Override
  public Future<UuidCollection> getSkippedInstancesIdsCollection(String requestId,
      int offset, int limit, String tenantId) {

    return getUuidCollectionForTable(
        SKIPPED_INSTANCES_IDS.REQUEST_ID,
        SKIPPED_INSTANCES_IDS.INSTANCE_ID,
        requestId, offset, limit, tenantId);
  }

  @Override
  public Future<UuidCollection> getFailedInstancesIdsCollection(String requestId,
      int offset, int limit, String tenantId) {

    return getUuidCollectionForTable(
        FAILED_INSTANCES_IDS.REQUEST_ID,
        FAILED_INSTANCES_IDS.INSTANCE_ID,
        requestId, offset, limit, tenantId);
  }

  @Override
  public Future<UuidCollection> getSuppressedInstancesIdsCollection(String requestId,
      int offset, int limit, String tenantId) {

    return getUuidCollectionForTable(
        SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS.REQUEST_ID,
        SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS.INSTANCE_ID,
        requestId, offset, limit, tenantId);
  }

  private Future<UuidCollection> getUuidCollectionForTable(
      TableField<?, UUID> requestIdField,
      TableField<?, UUID> instanceIdField,
      String requestId,
      int offset,
      int limit,
      String tenantId) {

    var uuid = UUID.fromString(requestId);

    var table = requestIdField.getTable();

    var count = JOOQ
        .selectCount()
        .from(table)
        .where(requestIdField.eq(uuid));

    var data = JOOQ
        .select(instanceIdField)
        .from(table)
        .where(requestIdField.eq(uuid))
        .offset(offset)
        .limit(limit);

    var client = reader(tenantId);

    return execute(client, count)
        .compose(countRs -> {
          int total = countRs.iterator().next().getInteger(0);
          return execute(client, data)
              .map(results -> {
                List<String> ids = new ArrayList<>();
                for (Row row : results) {
                  ids.add(row.getUUID(instanceIdField.getName()).toString());
                }
                return new UuidCollection()
                    .withUuidCollection(ids)
                    .withTotalRecords(total);
              });
        });
  }

  @Override
  public Future<RequestMetadataLb> saveRequestMetadata(RequestMetadataLb requestMetadata,
      String tenantId) {

    var uuid = requestMetadata.getRequestId();
    requestMetadata.setStreamEnded(false);

    if (Objects.isNull(uuid) || StringUtils.isEmpty(uuid.toString())) {
      return Future.failedFuture(
          new IllegalStateException("Cannot save request metadata, request metadata entity "
              + "must contain requestId"));
    }
    var startedDate = requestMetadata.getStartedDate();

    if (Objects.isNull(startedDate) || StringUtils.isEmpty(startedDate.toString())) {
      return Future.failedFuture(
          new IllegalStateException("Cannot save request metadata, request metadata entity "
              + "must contain startedDate"));
    }

    var insert = JOOQ
        .insertInto(REQUEST_METADATA_LB)
        .set(REQUEST_METADATA_LB.REQUEST_ID, requestMetadata.getRequestId())
        .set(REQUEST_METADATA_LB.STARTED_DATE, requestMetadata.getStartedDate())
        .set(REQUEST_METADATA_LB.LAST_UPDATED_DATE, requestMetadata.getLastUpdatedDate())
        .set(REQUEST_METADATA_LB.STREAM_ENDED, requestMetadata.getStreamEnded())
        .set(REQUEST_METADATA_LB.LINK_TO_ERROR_FILE, requestMetadata.getLinkToErrorFile())
        .set(REQUEST_METADATA_LB.PATH_TO_ERROR_FILE_IN_S3,
            requestMetadata.getPathToErrorFileInS3());

    return execute(writer(tenantId), insert)
        .map(v -> requestMetadata);
  }

  @Override
  public Future<RequestMetadataLb> updateRequestUpdatedDateAndStatistics(String requestId,
      OffsetDateTime lastUpdatedDate, StatisticsHolder holder, String tenantId) {

    var uuid = UUID.fromString(requestId);

    var update = JOOQ
        .update(REQUEST_METADATA_LB)
        .set(REQUEST_METADATA_LB.LAST_UPDATED_DATE, lastUpdatedDate)
        .set(REQUEST_METADATA_LB.DOWNLOADED_AND_SAVED_INSTANCES_COUNTER,
            REQUEST_METADATA_LB.DOWNLOADED_AND_SAVED_INSTANCES_COUNTER
                .plus(holder.getDownloadedAndSavedInstancesCounter().get()))
        .set(REQUEST_METADATA_LB.FAILED_TO_SAVE_INSTANCES_COUNTER,
            REQUEST_METADATA_LB.FAILED_TO_SAVE_INSTANCES_COUNTER
                .plus(holder.getFailedToSaveInstancesCounter().get()))
        .set(REQUEST_METADATA_LB.RETURNED_INSTANCES_COUNTER,
            REQUEST_METADATA_LB.RETURNED_INSTANCES_COUNTER
                .plus(holder.getReturnedInstancesCounter().get()))
        .set(REQUEST_METADATA_LB.SKIPPED_INSTANCES_COUNTER,
            REQUEST_METADATA_LB.SKIPPED_INSTANCES_COUNTER
                .plus(holder.getSkippedInstancesCounter().get()))
        .set(REQUEST_METADATA_LB.FAILED_INSTANCES_COUNTER,
            REQUEST_METADATA_LB.FAILED_INSTANCES_COUNTER
                .plus(holder.getFailedInstancesCounter().get()))
        .set(REQUEST_METADATA_LB.SUPPRESSED_INSTANCES_COUNTER,
            REQUEST_METADATA_LB.SUPPRESSED_INSTANCES_COUNTER
                .plus(holder.getSuppressedFromDiscoveryCounter().get()))
        .where(REQUEST_METADATA_LB.REQUEST_ID.eq(uuid))
        .returning();

    var client = writer(tenantId);

    return client.withTransaction(conn -> {
      var updateFuture = execute(conn, update)
          .map(rows -> singleRequestMetadataLbOrThrow(rows, requestId));

      var saveFailedToSaveInstancesIds =
          insertInstanceIds(conn,
              FAILED_TO_SAVE_INSTANCES_IDS,
              FAILED_TO_SAVE_INSTANCES_IDS.ID,
              FAILED_TO_SAVE_INSTANCES_IDS.REQUEST_ID,
              FAILED_TO_SAVE_INSTANCES_IDS.INSTANCE_ID,
              uuid,
              holder.getFailedToSaveInstancesIds());

      var saveFailedInstancesIds =
          insertInstanceIds(conn,
              FAILED_INSTANCES_IDS,
              FAILED_INSTANCES_IDS.ID,
              FAILED_INSTANCES_IDS.REQUEST_ID,
              FAILED_INSTANCES_IDS.INSTANCE_ID,
              uuid,
              holder.getFailedInstancesIds());

      var saveSkippedInstancesIds =
          insertInstanceIds(conn,
              SKIPPED_INSTANCES_IDS,
              SKIPPED_INSTANCES_IDS.ID,
              SKIPPED_INSTANCES_IDS.REQUEST_ID,
              SKIPPED_INSTANCES_IDS.INSTANCE_ID,
              uuid,
              holder.getSkippedInstancesIds());

      var saveSuppressedInstancesIds =
          insertInstanceIds(conn,
              SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS,
              SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS.ID,
              SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS.REQUEST_ID,
              SUPPRESSED_FROM_DISCOVERY_INSTANCES_IDS.INSTANCE_ID,
              uuid,
              holder.getSuppressedInstancesIds());

      return updateFuture.compose(updated ->
          Future
              .all(saveFailedToSaveInstancesIds, saveFailedInstancesIds,
                  saveSkippedInstancesIds, saveSuppressedInstancesIds)
              .map(v -> updated)
      );
    });
  }

  private <R extends Record> Future<Void> insertInstanceIds(
      SqlConnection conn,
      Table<R> table,
      TableField<R, UUID> idField,
      TableField<R, UUID> requestIdField,
      TableField<R, UUID> instanceIdField,
      UUID requestId,
      List<String> instanceIds) {

    if (instanceIds == null || instanceIds.isEmpty()) {
      return Future.succeededFuture();
    }

    var insert =
        JOOQ.insertInto(table, idField, requestIdField, instanceIdField);

    for (String instId : instanceIds) {
      insert = insert.values(UUID.randomUUID(), requestId, UUID.fromString(instId));
    }

    return execute(conn, insert).mapEmpty();
  }

  @Deprecated(since = "3.7.2", forRemoval = true)
  @Override
  public Future<RequestMetadataLb> updateRequestStreamEnded(String requestId,
      boolean isStreamEnded, String tenantId) {

    var uuid = UUID.fromString(requestId);

    var query = JOOQ
        .update(REQUEST_METADATA_LB)
        .set(REQUEST_METADATA_LB.STREAM_ENDED, isStreamEnded)
        .where(REQUEST_METADATA_LB.REQUEST_ID.eq(uuid))
        .returning();

    return execute(writer(tenantId), query)
        .map(rows -> singleRequestMetadataLbOrThrow(rows, requestId));
  }

  @Override
  public Future<Boolean> deleteRequestMetadataByRequestId(String requestId, String tenantId) {
    var uuid = UUID.fromString(requestId);
    var query = JOOQ
        .deleteFrom(REQUEST_METADATA_LB)
        .where(REQUEST_METADATA_LB.REQUEST_ID.eq(uuid));

    return execute(writer(tenantId), query)
        .map(results -> {
          if (results.rowCount() == 1) {
            logger.debug("Request metadata with id '{}' has been deleted.", requestId);
            return true;
          }
          logger.error("Cannot delete request metadata with id {}.", requestId);
          throw new NotFoundException(
              String.format(REQUEST_METADATA_WITH_ID_DOES_NOT_EXIST, requestId));
        });
  }

  @Override
  public Future<Boolean> deleteInstancesById(List<String> instIds, String requestId,
      String tenantId) {

    var uuid = UUID.fromString(requestId);
    var uuids = instIds.stream()
        .map(UUID::fromString)
        .collect(toList());

    var query = JOOQ
        .deleteFrom(INSTANCES)
        .where(INSTANCES.REQUEST_ID.eq(uuid)
            .and(INSTANCES.INSTANCE_ID.in(uuids)));

    return execute(writer(tenantId), query)
        .map(results -> {
          String instanceIds = String.join(",", instIds);
          int deleted = results.rowCount();
          if (deleted > 0) {
            logger.debug("Instances with ids [{}] have been removed.", instanceIds);
            return true;
          } else {
            logger.debug("Cannot delete instances: there no instances with id's - [{}].",
                instanceIds);
            return false;
          }
        });
  }

  @Override
  public Future<Void> saveInstances(List<Instances> instances, String tenantId) {
    if (instances.isEmpty()) {
      logger.debug("Skip saving instances. Instances list is empty.");
      return Future.succeededFuture();
    }

    var insert =
        JOOQ.insertInto(INSTANCES, INSTANCES.INSTANCE_ID,
            INSTANCES.REQUEST_ID, INSTANCES.SUPPRESS_FROM_DISCOVERY);

    for (Instances instance : instances) {
      insert = insert.values(instance.getInstanceId(),
          instance.getRequestId(),
          instance.getSuppressFromDiscovery());
    }

    return execute(writer(tenantId), insert).mapEmpty();
  }

  @Override
  public Future<List<Instances>> getInstancesList(int limit, String requestId, String tenantId) {

    var uuid = UUID.fromString(requestId);

    var query = JOOQ
        .selectFrom(INSTANCES)
        .where(INSTANCES.REQUEST_ID.eq(uuid))
        .orderBy(INSTANCES.ID)
        .limit(limit);

    return execute(reader(tenantId), query)
        .map(this::rowSetToInstancesList);
  }

  @Override
  public Future<RequestMetadataLb> updateRequestMetadataByPathToError(String requestId,
      String tenantId, String pathToErrorFile) {

    var uuid = UUID.fromString(requestId);

    var query = JOOQ
        .update(REQUEST_METADATA_LB)
        .set(REQUEST_METADATA_LB.PATH_TO_ERROR_FILE_IN_S3, pathToErrorFile)
        .where(REQUEST_METADATA_LB.REQUEST_ID.eq(uuid))
        .returning();

    return execute(writer(tenantId), query)
        .map(rows -> singleRequestMetadataLbOrThrow(rows, requestId));
  }

  @Override
  public Future<RequestMetadataLb> updateRequestMetadataByLinkToError(String requestId,
      String tenantId, String linkToError) {

    var uuid = UUID.fromString(requestId);

    var query = JOOQ
        .update(REQUEST_METADATA_LB)
        .set(REQUEST_METADATA_LB.LINK_TO_ERROR_FILE, linkToError)
        .where(REQUEST_METADATA_LB.REQUEST_ID.eq(uuid))
        .returning();

    return execute(writer(tenantId), query)
        .map(rows -> singleRequestMetadataLbOrThrow(rows, requestId));
  }

  @Override
  public Future<List<String>> getRequestMetadataIdsByStartedDateAndExistsByPathToErrorFileInS3(
      String tenantId, OffsetDateTime date) {

    var query = JOOQ
        .select(REQUEST_METADATA_LB.REQUEST_ID, REQUEST_METADATA_LB.STARTED_DATE)
        .from(REQUEST_METADATA_LB)
        .where(REQUEST_METADATA_LB.PATH_TO_ERROR_FILE_IN_S3.isNotNull())
        .and(REQUEST_METADATA_LB.PATH_TO_ERROR_FILE_IN_S3.ne(""));

    return execute(reader(tenantId), query)
        .map(results -> {
          List<String> ids = new ArrayList<>();
          for (Row row : results) {
            var started = row.getOffsetDateTime(C_STARTED_DATE);
            if (started != null && started.isBefore(date)) {
              ids.add(row.getUUID(C_REQUEST_ID).toString());
            }
          }
          return ids;
        });
  }

  private List<String> mapRequestIdsRowSetToList(RowSet<Row> rows) {
    List<String> ids = new ArrayList<>();
    for (Row row : rows) {
      ids.add(row.getUUID(C_REQUEST_ID).toString());
    }
    var idList = String.join(",", ids);
    logger.debug("Expired request ids result: [{}].", idList);
    return ids;
  }

  private RequestMetadataLb singleRequestMetadataLbOrThrow(RowSet<Row> rows,
      String requestIdForError) {
    if (!rows.iterator().hasNext()) {
      throw new NotFoundException(
          String.format(REQUEST_METADATA_WITH_ID_DOES_NOT_EXIST, requestIdForError));
    }
    var row = rows.iterator().next();
    return rowToRequestMetadataLb(row)
        .orElseThrow(() -> new NotFoundException(
            String.format(REQUEST_METADATA_WITH_ID_DOES_NOT_EXIST, requestIdForError)));
  }

  private Optional<RequestMetadataLb> rowToRequestMetadataLb(Row row) {
    if (row == null) {
      return Optional.empty();
    }
    var requestMetadataLb = new RequestMetadataLb();
    requestMetadataLb.setRequestId(row.getUUID(C_REQUEST_ID));
    requestMetadataLb.setLastUpdatedDate(row.getOffsetDateTime(C_LAST_UPDATED_DATE));
    requestMetadataLb.setStreamEnded(row.getBoolean(C_STREAM_ENDED));
    requestMetadataLb.setLinkToErrorFile(row.getString(C_LINK_TO_ERROR_FILE));
    requestMetadataLb.setPathToErrorFileInS3(row.getString(C_PATH_TO_ERROR_FILE_IN_S3));
    requestMetadataLb.setStartedDate(row.getOffsetDateTime(C_STARTED_DATE));
    requestMetadataLb.setDownloadedAndSavedInstancesCounter(row.getInteger(C_DOWNLOADED_AND_SAVED));
    requestMetadataLb.setFailedToSaveInstancesCounter(row.getInteger(C_FAILED_TO_SAVE));
    requestMetadataLb.setReturnedInstancesCounter(row.getInteger(C_RETURNED));
    requestMetadataLb.setSkippedInstancesCounter(row.getInteger(C_SKIPPED));
    requestMetadataLb.setFailedInstancesCounter(row.getInteger(C_FAILED));
    requestMetadataLb.setSuppressedInstancesCounter(row.getInteger(C_SUPPRESSED));
    return of(requestMetadataLb);
  }

  private List<Instances> rowSetToInstancesList(RowSet<Row> rows) {
    return rows.stream()
        .map(this::rowToInstances)
        .collect(toList());
  }

  private Instances rowToInstances(Row row) {
    var instances = new Instances();
    instances.setInstanceId(row.getUUID(C_INSTANCE_ID));
    instances.setRequestId(row.getUUID(C_REQUEST_ID));
    instances.setSuppressFromDiscovery(row.getBoolean(C_SUPPRESS_FROM_DISCOVERY));
    instances.setId(row.getInteger(C_ID));
    return instances;
  }

  private RequestMetadata rowToRequestMetadata(Row row) {
    var opt = rowToRequestMetadataLb(row);
    var pojo = opt.orElse(null);
    var requestMetadata = new RequestMetadata();

    if (pojo != null) {
      of(pojo.getRequestId())
          .ifPresent(uuid -> requestMetadata.withRequestId(uuid.toString()));
      of(pojo.getLastUpdatedDate())
          .ifPresent(offsetDateTime -> requestMetadata.withLastUpdatedDate(
              Date.from(offsetDateTime.toInstant())));
      of(pojo.getStreamEnded()).ifPresent(requestMetadata::withStreamEnded);
      of(pojo.getDownloadedAndSavedInstancesCounter())
          .ifPresent(requestMetadata::withDownloadedAndSavedInstancesCounter);
      of(pojo.getFailedToSaveInstancesCounter())
          .ifPresent(requestMetadata::withFailedToSaveInstancesCounter);
      of(pojo.getReturnedInstancesCounter())
          .ifPresent(requestMetadata::withReturnedInstancesCounter);
      of(pojo.getFailedInstancesCounter())
          .ifPresent(requestMetadata::withFailedInstancesCounter);
      of(pojo.getSkippedInstancesCounter())
          .ifPresent(requestMetadata::withSkippedInstancesCounter);
      of(pojo.getSuppressedInstancesCounter())
          .ifPresent(requestMetadata::withSuppressedInstancesCounter);
      ofNullable(pojo.getPathToErrorFileInS3()).ifPresentOrElse(pathToError -> {
        if (!pathToError.isEmpty()) {
          requestMetadata.withLinkToErrorFile(pathToError);
        } else {
          requestMetadata.setLinkToErrorFile("");
        }
      }, () -> requestMetadata.setLinkToErrorFile(""));
      of(pojo.getStartedDate())
          .ifPresent(offsetDateTime -> requestMetadata.withStartedDate(
              Date.from(offsetDateTime.toInstant())));
    }

    return requestMetadata;
  }

  private Pool writer(String tenantId) {
    return postgresClientFactory.getPoolWriter(tenantId);
  }

  private Pool reader(String tenantId) {
    return postgresClientFactory.getPoolReader(tenantId);
  }

  private Future<RowSet<Row>> execute(Pool pool, Query query) {
    var sql = query.getSQL(ParamType.INLINED);
    return pool.query(sql).execute();
  }

  private Future<RowSet<Row>> execute(SqlConnection connection, Query query) {
    var sql = query.getSQL(ParamType.INLINED);
    return connection.query(sql).execute();
  }
}