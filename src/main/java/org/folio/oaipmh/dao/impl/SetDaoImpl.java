package org.folio.oaipmh.dao.impl;

import static java.util.Date.from;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.folio.rest.jooq.Tables.SET_LB;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import org.apache.commons.lang.StringUtils;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.dao.SetDao;
import org.folio.rest.jaxrs.model.FolioSet;
import org.folio.rest.jaxrs.model.FolioSetCollection;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.records.SetLbRecord;
import org.jooq.Condition;
import org.springframework.stereotype.Repository;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

@Repository
public class SetDaoImpl implements SetDao {

  private static final String ALREADY_EXISTS_ERROR_MSG = "Set with id '%s' already exists";
  private static final String NOT_FOUND_ERROR_MSG = "Set with id '%s' was not found";

  private final PostgresClientFactory postgresClientFactory;

  public SetDaoImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<FolioSet> getSetById(String id, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      Condition condition = SET_LB.ID.eq(UUID.fromString(id));
      return txQE.findOneRow(dslContext -> dslContext.selectFrom(SET_LB)
        .where(condition)
        .limit(1))
        .map(this::toOptionalSet)
        .map(optionalSet -> {
          if (optionalSet.isPresent()) {
            return optionalSet.get();
          }
          throw new NotFoundException(String.format(NOT_FOUND_ERROR_MSG, id));
        });
    });
  }

  @Override
  public Future<FolioSet> updateSetById(String id, FolioSet entry, String tenantId, String userId) {
    entry.setId(id);
    prepareSetMetadata(entry, userId, InsertType.UPDATE);
    SetLbRecord dbRecord = toDatabaseSetRecord(entry);
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.executeAny(dslContext -> dslContext.update(SET_LB)
      .set(dbRecord)
      .where(SET_LB.ID.eq(UUID.fromString(entry.getId())))
      .returning())
      .map(this::toOptionalSet)
      .map(optionalSet -> {
        if (optionalSet.isPresent()) {
          return optionalSet.get();
        }
        throw new NotFoundException(String.format(NOT_FOUND_ERROR_MSG, entry.getId()));
      }));
  }

  @Override
  public Future<FolioSet> saveSet(FolioSet entry, String tenantId, String userId) {
    if (StringUtils.isNotEmpty(entry.getId())) {
      return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.execute(dslContext -> dslContext.selectFrom(SET_LB)
        .where(SET_LB.ID.eq(UUID.fromString(entry.getId()))))
        .compose(res -> {
          if (res == 1) {
            throw new IllegalArgumentException(String.format(ALREADY_EXISTS_ERROR_MSG, entry.getId()));
          }
          return saveSetItem(entry, tenantId, userId);
        }));
    } else {
      entry.setId(UUID.randomUUID()
        .toString());
      return saveSetItem(entry, tenantId, userId);
    }
  }

  private Future<FolioSet> saveSetItem(FolioSet entry, String tenantId, String userId) {
    prepareSetMetadata(entry, userId, InsertType.INSERT);
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.executeAny(dslContext -> dslContext.insertInto(SET_LB)
      .set(toDatabaseSetRecord(entry))
      .onConflict(SET_LB.ID)
      .doNothing()
      .returning())
      .map(raw -> entry));
  }

  @Override
  public Future<Boolean> deleteSetById(String id, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.execute(dslContext -> dslContext.deleteFrom(SET_LB)
      .where(SET_LB.ID.eq(UUID.fromString(id))))
      .map(res -> {
        if (res == 1) {
          return true;
        }
        throw new NotFoundException(String.format(NOT_FOUND_ERROR_MSG, id));
      }));
  }

  @Override
  public Future<FolioSetCollection> getSetList(int offset, int limit, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.query(dslContext -> dslContext.selectFrom(SET_LB)
      .offset(offset)
      .limit(limit))
      .map(this::queryResultToSetCollection));
  }

  private FolioSetCollection queryResultToSetCollection(QueryResult queryResult) {
    List<FolioSet> list = queryResult.stream()
      .map(row -> rowToSet(row.unwrap()))
      .collect(Collectors.toList());
    return new FolioSetCollection().withSets(list)
      .withTotalRecords(list.size());
  }

  private void prepareSetMetadata(FolioSet entry, String userId, InsertType insertType) {
    if (insertType.equals(InsertType.INSERT)) {
      entry.setCreatedDate(from(Instant.now()));
      entry.setCreatedByUserId(userId);
    }
    entry.setUpdatedDate(from(Instant.now()));
    entry.setUpdatedByUserId(userId);
  }

  private SetLbRecord toDatabaseSetRecord(FolioSet set) {
    SetLbRecord dbRecord = new SetLbRecord();
    dbRecord.setDescription(set.getDescription());
    if (isNotEmpty(set.getId())) {
      dbRecord.setId(UUID.fromString(set.getId()));
    }
    if (isNotEmpty(set.getName())) {
      dbRecord.setName(set.getName());
    }
    if (isNotEmpty(set.getSetSpec())) {
      dbRecord.setSetSpec(set.getSetSpec());
    }
    if (Objects.nonNull(set.getCreatedDate())) {
      dbRecord.setCreatedDate(set.getCreatedDate()
        .toInstant()
        .atOffset(ZoneOffset.UTC));
    }
    if (isNotEmpty(set.getCreatedByUserId())) {
      dbRecord.setCreatedByUserId(UUID.fromString(set.getCreatedByUserId()));
    }
    if (Objects.nonNull(set.getUpdatedDate())) {
      dbRecord.setUpdatedDate(set.getUpdatedDate()
        .toInstant()
        .atOffset(ZoneOffset.UTC));
    }
    if (isNotEmpty(set.getUpdatedByUserId())) {
      dbRecord.setUpdatedByUserId(UUID.fromString(set.getUpdatedByUserId()));
    }
    return dbRecord;
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }

  private Optional<FolioSet> toOptionalSet(Row row) {
    return nonNull(row) ? Optional.of(rowToSet(row)) : Optional.empty();
  }

  private Optional<FolioSet> toOptionalSet(RowSet<Row> rows) {
    return rows.rowCount() == 1 ? Optional.of(rowToSet(rows.iterator()
      .next())) : Optional.empty();
  }

  private FolioSet rowToSet(Row row) {
    org.folio.rest.jooq.tables.pojos.SetLb pojo = RowMappers.getSetLbMapper()
      .apply(row);
    FolioSet set = new FolioSet();
    if (nonNull(pojo.getId())) {
      set.withId(pojo.getId()
        .toString());
    }
    if (nonNull(pojo.getName())) {
      set.withName(pojo.getName());
    }
    if (nonNull(pojo.getDescription())) {
      set.withDescription(pojo.getDescription());
    }
    if (nonNull(pojo.getSetSpec())) {
      set.withSetSpec(pojo.getSetSpec());
    }
    if (nonNull(pojo.getCreatedByUserId())) {
      set.withCreatedByUserId(pojo.getCreatedByUserId()
        .toString());
    }
    if (nonNull(pojo.getCreatedDate())) {
      set.withCreatedDate(from(pojo.getCreatedDate()
        .toInstant()));
    }
    if (nonNull(pojo.getUpdatedByUserId())) {
      set.withUpdatedByUserId(pojo.getUpdatedByUserId()
        .toString());
    }
    if (nonNull(pojo.getUpdatedDate())) {
      set.withUpdatedDate(from(pojo.getUpdatedDate()
        .toInstant()));
    }
    return set;
  }

  private enum InsertType {
    INSERT, UPDATE
  }

}
