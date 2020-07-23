package org.folio.oaipmh.dao.impl;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.folio.rest.jooq.Tables.SET;

import java.sql.Date;
import java.util.Optional;
import java.util.UUID;

import javax.ws.rs.NotFoundException;

import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.dao.SetDao;
import org.folio.rest.jaxrs.model.Set;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.records.SetRecord;
import org.folio.spring.SpringContextUtil;
import org.jooq.Condition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

@Component
public class SetDaoImpl implements SetDao {

  private static final Logger logger = LoggerFactory.getLogger(SetDaoImpl.class);

  private final PostgresClientFactory postgresClientFactory;

  public SetDaoImpl(final PostgresClientFactory postgresClientFactory) {
    logger.info("SetDaoImpl constructor start");
    this.postgresClientFactory = postgresClientFactory;
    logger.info("SetDaoImpl constructor finish, postgresClientFactory - {}", this.postgresClientFactory);
  }

  @Override
  public Future<Optional<Set>> getSetById(String id, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      Condition condition = SET.ID.eq(UUID.fromString(id));
      return txQE.findOneRow(dslContext -> dslContext.selectFrom(SET)
        .where(condition)
        .limit(1))
        .map(this::toOptionalSet);
    });
  }

  @Override
  public Future<Set> updateSetById(String id, Set entry, String tenantId) {
    entry.setId(id);
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
        throw new NotFoundException(String.format("Set with id '%s' was not found", entry.getId()));
      }));
  }

  @Override
  public Future<Set> saveSet(Set entry, String tenantId) {
    entry.setId(UUID.randomUUID()
      .toString());
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.executeAny(dslContext -> dslContext.insertInto(SET)
      .set(toDatabaseSetRecord(entry))
      // later in validation ticket .onConflict(setSpec fild must be unqiue)
      .returning())
      .map(raw -> entry));
  }

  @Override
  public Future<Boolean> deleteSetById(String id, String tenantId) {
    return getQueryExecutor(tenantId).transaction(queryExecutor -> queryExecutor.execute(dslContext -> dslContext.deleteFrom(SET)
      .where(SET.ID.eq(UUID.fromString(id))))
      .map(res -> res == 1));
  }

  // public Future<Optional<Set>> getSetById(ReactiveClassicGenericQueryExecutor txQE, String id) {
//    Condition condition = SET.ID.eq(UUID.fromString(id));
//    return txQE.findOneRow(dslContext -> dslContext.selectFrom(SET)
//      .where(condition)
//      .limit(1))
//      .map(this::toOptionalSet);
//  }

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
    return dbRecord;
  }

//  public Future<Set> insertOrUpdateSet(ReactiveClassicGenericQueryExecutor queryExecutor, Set set) {
//    Field<UUID> idField = field(name("id"), UUID.class);
//    Field<String> nameField = field(name("name"), String.class);
//    Field<String> descriptionField = field(name("description"), String.class);
//    Field<String> setSpecField = field(name("set_spec"), String.class);
//    UUID setId = UUID.fromString(set.getId());
//    return queryExecutor.executeAny(dslContext -> dslContext.insertInto((table(name("set"))))
//      .set(idField, setId)
//      .set(nameField, set.getName())
//      .set(descriptionField, set.getDescription())
//      .set(setSpecField, set.getSetSpec())
//    .onConflict(idField)
//    .doUpdate()
//      .set(nameField, set.getName())
//      .set(descriptionField, set.getDescription())
//      .set(setSpecField, set.getSetSpec())
//    .returning())
//      .map(raw -> {
//        if(raw.rowCount())
//      })
//  }

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

  private static Set rowToSet(Row row) {
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
      set.withCreatedDate(Date.from(pojo.getCreatedDate()
        .toInstant()));
    }
    if (nonNull(pojo.getUpdatedByUserId())) {
      set.withUpdatedByUserId(pojo.getUpdatedByUserId()
        .toString());
    }
    if (nonNull(pojo.getUpdatedDate())) {
      set.withUpdatedDate(Date.from(pojo.getUpdatedDate()
        .toInstant()));
    }
    return set;
  }

}
