package org.folio.oaipmh.dao.impl;

import static java.util.Date.from;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.folio.rest.jooq.Tables.SET;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import javax.ws.rs.NotFoundException;

import org.apache.commons.lang.StringUtils;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.dao.SetDao;
import org.folio.rest.jaxrs.model.Set;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.records.SetRecord;
import org.jooq.Condition;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

@Repository
public class SetDaoImpl implements SetDao {

  private static final Logger logger = LoggerFactory.getLogger(SetDaoImpl.class);

  private static final String ALREADY_EXISTS_ERROR_MSG = "Set with id '%s' already exists";
  private static final String NOT_FOUND_ERROR_MSG = "Set with id '%s' was not found";

  private final PostgresClientFactory postgresClientFactory;

  public SetDaoImpl(final PostgresClientFactory postgresClientFactory) {
    logger.info("SetDaoImpl constructor start");
    this.postgresClientFactory = postgresClientFactory;
    logger.info("SetDaoImpl constructor finish, postgresClientFactory - {}", this.postgresClientFactory);
  }

  @Override
  public Future<Set> getSetById(String id, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      Condition condition = SET.ID.eq(UUID.fromString(id));
      return txQE.findOneRow(dslContext -> dslContext.selectFrom(SET)
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
  public Future<Set> updateSetById(String id, Set entry, String tenantId, String userId) {
    entry.setId(id);
    prepareSetMetadata(entry, userId, InsertType.UPDATE);
    SetRecord dbRecord = toDatabaseSetRecord(entry);
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.executeAny(dslContext -> dslContext.update(SET)
      .set(dbRecord)
      .where(SET.ID.eq(UUID.fromString(entry.getId())))
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
  public Future<Set> saveSet(Set entry, String tenantId, String userId) {
    if (StringUtils.isNotEmpty(entry.getId())) {
      return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.execute(dslContext -> dslContext.selectFrom(SET)
        .where(SET.ID.eq(UUID.fromString(entry.getId()))))
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

  private Future<Set> saveSetItem(Set entry, String tenantId, String userId) {
    prepareSetMetadata(entry, userId, InsertType.INSERT);
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.executeAny(dslContext -> dslContext.insertInto(SET)
      .set(toDatabaseSetRecord(entry))
      .onConflict(SET.ID)
      .doNothing()
      .returning())
      .map(raw -> entry));
  }

  @Override
  public Future<Boolean> deleteSetById(String id, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.execute(dslContext -> dslContext.deleteFrom(SET)
      .where(SET.ID.eq(UUID.fromString(id))))
      .map(res -> {
        if (res == 1) {
          return true;
        }
        throw new NotFoundException(String.format(NOT_FOUND_ERROR_MSG, id));
      }));
  }

  private void prepareSetMetadata(Set entry, String userId, InsertType insertType) {
    if (insertType.equals(InsertType.UPDATE)) {
      entry.setUpdatedDate(from(Instant.now()));
      entry.setUpdatedByUserId(userId);
    } else {
      entry.setCreatedDate(from(Instant.now()));
      entry.setCreatedByUserId(userId);
    }
  }

  private SetRecord toDatabaseSetRecord(Set set) {
    SetRecord dbRecord = new SetRecord();
    if (isNotEmpty(set.getId())) {
      dbRecord.setId(UUID.fromString(set.getId()));
    }
    if (isNotEmpty(set.getName())) {
      dbRecord.setName(set.getName());
    }
    if (isNotEmpty(set.getDescription())) {
      dbRecord.setDescription(set.getDescription());
    }
    if (isNotEmpty(set.getSetSpec())) {
      dbRecord.setSetspec(set.getSetSpec());
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

  private Optional<Set> toOptionalSet(Row row) {
    return nonNull(row) ? Optional.of(rowToSet(row)) : Optional.empty();
  }

  private Optional<Set> toOptionalSet(RowSet<Row> rows) {
    return rows.rowCount() == 1 ? Optional.of(rowToSet(rows.iterator()
      .next())) : Optional.empty();
  }

  private Set rowToSet(Row row) {
    org.folio.rest.jooq.tables.pojos.Set pojo = RowMappers.getSetMapper()
      .apply(row);
    Set set = new Set();
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
    if (nonNull(pojo.getSetspec())) {
      set.withSetSpec(pojo.getSetspec());
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
