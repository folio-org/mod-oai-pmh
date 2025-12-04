package org.folio.oaipmh.dao.impl;

import static java.util.Date.from;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.rest.jooq.Tables.SET_LB;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.ws.rs.NotFoundException;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.dao.SetDao;
import org.folio.rest.jaxrs.model.FilteringCondition;
import org.folio.rest.jaxrs.model.FolioSet;
import org.folio.rest.jaxrs.model.FolioSetCollection;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

@Repository
public class SetDaoImpl implements SetDao {

  private static final String ALREADY_EXISTS_ERROR_MSG = "Set with id '%s' already exists";
  private static final String NOT_FOUND_ERROR_MSG = "Set with id '%s' was not found";
  private static final DSLContext JOOQ = DSL.using(
      SQLDialect.POSTGRES,
      new Settings()
          .withParamType(ParamType.NAMED)
  );

  private final PostgresClientFactory postgresClientFactory;

  public SetDaoImpl(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<FolioSet> getSetById(String id, String tenantId) {

    var uuid = UUID.fromString(id);
    var query = JOOQ
        .selectFrom(SET_LB)
        .where(SET_LB.ID.eq(uuid))
        .limit(1);
    var sqlAndParams = toSql(query);
    var client = postgresClientFactory.getPoolReader(tenantId);
    Promise<FolioSet> promise = Promise.promise();

    client.preparedQuery(sqlAndParams.sql())
        .execute(sqlAndParams.params())
        .onSuccess(rows -> {
          var opt = toOptionalSet(rows);
          if (opt.isPresent()) {
            promise.complete(opt.get());
          } else {
            promise.fail(new NotFoundException(String.format(NOT_FOUND_ERROR_MSG, id)));
          }
        })
        .onFailure(promise::fail);

    return promise.future();
  }

  @Override
  public Future<FolioSet> updateSetById(String id, FolioSet entry, String tenantId, String userId) {

    entry.setId(id);
    prepareSetMetadata(entry, userId, InsertType.UPDATE);

    var uuid = UUID.fromString(entry.getId());
    var query = JOOQ
        .update(SET_LB)
        .set(SET_LB.NAME, entry.getName())
        .set(SET_LB.DESCRIPTION, entry.getDescription())
        .set(SET_LB.SET_SPEC, entry.getSetSpec())
        .set(SET_LB.FILTERING_CONDITIONS,
            nonNull(entry.getFilteringConditions())
                ? fkListToJsonString(entry.getFilteringConditions())
                : null)
        .set(SET_LB.CREATED_DATE,
            nonNull(entry.getCreatedDate())
                ? entry.getCreatedDate().toInstant().atOffset(ZoneOffset.UTC)
                : null)
        .set(SET_LB.CREATED_BY_USER_ID,
            isNotEmpty(entry.getCreatedByUserId())
                ? UUID.fromString(entry.getCreatedByUserId())
                : null)
        .set(SET_LB.UPDATED_DATE,
            nonNull(entry.getUpdatedDate())
                ? entry.getUpdatedDate().toInstant().atOffset(ZoneOffset.UTC)
                : null)
        .set(SET_LB.UPDATED_BY_USER_ID,
            isNotEmpty(entry.getUpdatedByUserId())
                ? UUID.fromString(entry.getUpdatedByUserId())
                : null)
        .where(SET_LB.ID.eq(uuid))
        .returning();

    var sqlAndParams = toSql(query);
    var client = postgresClientFactory.getPoolWriter(tenantId);
    Promise<FolioSet> promise = Promise.promise();

    client.preparedQuery(sqlAndParams.sql())
        .execute(sqlAndParams.params())
        .onSuccess(rows -> {
          Optional<FolioSet> opt = toOptionalSet(rows);
          if (opt.isPresent()) {
            promise.complete(opt.get());
          } else {
            promise.fail(new NotFoundException(String.format(NOT_FOUND_ERROR_MSG, entry.getId())));
          }
        })
        .onFailure(promise::fail);

    return promise.future();
  }

  @Override
  public Future<FolioSet> saveSet(FolioSet entry, String tenantId, String userId) {
    var client = postgresClientFactory.getPoolWriter(tenantId);

    if (isNotEmpty(entry.getId())) {
      var uuid = UUID.fromString(entry.getId());
      var checkQuery = JOOQ
          .selectOne()
          .from(SET_LB)
          .where(SET_LB.ID.eq(uuid));
      var check = toSql(checkQuery);

      return client.preparedQuery(check.sql())
          .execute(check.params())
          .compose(rows -> {
            if (rows.size() != 0) {
              return Future.failedFuture(
                  new IllegalArgumentException(String.format(ALREADY_EXISTS_ERROR_MSG,
                      entry.getId())));
            }
            return saveSetItem(entry, tenantId, userId);
          });
    } else {
      entry.setId(UUID.randomUUID().toString());
      return saveSetItem(entry, tenantId, userId);
    }
  }

  @Override
  public Future<Boolean> deleteSetById(String id, String tenantId) {
    var uuid = UUID.fromString(id);
    var query = JOOQ
        .deleteFrom(SET_LB)
        .where(SET_LB.ID.eq(uuid));
    var sqlAndParams = toSql(query);
    var client = postgresClientFactory.getPoolWriter(tenantId);
    Promise<Boolean> promise = Promise.promise();

    client.preparedQuery(sqlAndParams.sql())
        .execute(sqlAndParams.params())
        .onSuccess(rows -> {
          if (rows.rowCount() == 1) {
            promise.complete(true);
          } else {
            promise.fail(new NotFoundException(String.format(NOT_FOUND_ERROR_MSG, id)));
          }
        })
        .onFailure(promise::fail);

    return promise.future();
  }

  @Override
  public Future<FolioSetCollection> getSetList(int offset, int limit, String tenantId) {
    var client = postgresClientFactory.getPoolReader(tenantId);

    var countQuery = JOOQ
        .selectCount()
        .from(SET_LB);

    var count = toSql(countQuery);

    ResultQuery<?> query = JOOQ
        .selectFrom(SET_LB)
        .orderBy(SET_LB.UPDATED_DATE.desc())
        .offset(offset)
        .limit(limit);

    var data = toSql(query);

    Promise<FolioSetCollection> promise = Promise.promise();

    client.preparedQuery(count.sql())
        .execute(count.params())
        .compose(rows -> {
          int total = rows.iterator().next().getInteger(0);

          Promise<FolioSetCollection> dataPromise = Promise.promise();
          client.preparedQuery(data.sql())
              .execute(data.params())
              .onSuccess(dataRows -> {
                var sets = rowsToSetList(dataRows);
                var collection = new FolioSetCollection()
                    .withSets(sets)
                    .withTotalRecords(total);
                dataPromise.complete(collection);
              })
              .onFailure(dataPromise::fail);
          return dataPromise.future();
        })
        .onSuccess(promise::complete)
        .onFailure(promise::fail);

    return promise.future();
  }

  private Future<FolioSet> saveSetItem(FolioSet entry, String tenantId, String userId) {

    prepareSetMetadata(entry, userId, InsertType.INSERT);

    var uuid = UUID.fromString(entry.getId());

    var query = JOOQ
        .insertInto(SET_LB)
        .set(SET_LB.ID, uuid)
        .set(SET_LB.NAME, entry.getName())
        .set(SET_LB.DESCRIPTION, entry.getDescription())
        .set(SET_LB.SET_SPEC, entry.getSetSpec())
        .set(SET_LB.FILTERING_CONDITIONS,
            nonNull(entry.getFilteringConditions())
                ? fkListToJsonString(entry.getFilteringConditions())
                : null)
        .set(SET_LB.CREATED_DATE,
            nonNull(entry.getCreatedDate())
                ? entry.getCreatedDate().toInstant().atOffset(ZoneOffset.UTC)
                : null)
        .set(SET_LB.CREATED_BY_USER_ID,
            isNotEmpty(entry.getCreatedByUserId())
                ? UUID.fromString(entry.getCreatedByUserId())
                : null)
        .set(SET_LB.UPDATED_DATE,
            nonNull(entry.getUpdatedDate())
                ? entry.getUpdatedDate().toInstant().atOffset(ZoneOffset.UTC)
                : null)
        .set(SET_LB.UPDATED_BY_USER_ID,
            isNotEmpty(entry.getUpdatedByUserId())
                ? UUID.fromString(entry.getUpdatedByUserId())
                : null);
    var sqlAndParams = toSql(query);
    var client = postgresClientFactory.getPoolWriter(tenantId);
    Promise<FolioSet> promise = Promise.promise();

    client.preparedQuery(sqlAndParams.sql())
        .execute(sqlAndParams.params())
        .onSuccess(rows -> promise.complete(entry))
        .onFailure(promise::fail);

    return promise.future();
  }

  private void prepareSetMetadata(FolioSet entry, String userId, InsertType insertType) {
    if (insertType == InsertType.INSERT) {
      entry.setCreatedDate(from(Instant.now()));
      entry.setCreatedByUserId(userId);
    }
    if (isNull(entry.getCreatedByUserId())) {
      entry.setCreatedByUserId(userId);
    }
    if (isNull(entry.getCreatedDate())) {
      entry.setCreatedDate(from(Instant.now()));
    }
    entry.setUpdatedDate(from(Instant.now()));
    entry.setUpdatedByUserId(userId);
  }

  private Optional<FolioSet> toOptionalSet(RowSet<Row> rows) {
    if (rows == null || rows.size() == 0) {
      return Optional.empty();
    }
    return Optional.of(rowToSet(rows.iterator().next()));
  }

  private List<FolioSet> rowsToSetList(RowSet<Row> rows) {
    return rows.stream()
        .map(this::rowToSet)
        .toList();
  }

  private FolioSet rowToSet(Row row) {
    FolioSet set = new FolioSet();

    UUID id = row.getUUID("id");
    if (nonNull(id)) {
      set.withId(id.toString());
    }

    String name = row.getString("name");
    if (nonNull(name)) {
      set.withName(name);
    }

    String description = row.getString("description");
    if (nonNull(description)) {
      set.withDescription(description);
    }

    String setSpec = row.getString("set_spec");
    if (nonNull(setSpec)) {
      set.withSetSpec(setSpec);
    }

    String filteringJson = row.getString("filtering_conditions");
    if (nonNull(filteringJson)) {
      set.setFilteringConditions(jsonStringToFkList(filteringJson));
    }

    UUID createdBy = row.getUUID("created_by_user_id");
    if (nonNull(createdBy)) {
      set.withCreatedByUserId(createdBy.toString());
    }

    OffsetDateTime createdDate = row.getOffsetDateTime("created_date");
    if (nonNull(createdDate)) {
      set.withCreatedDate(from(createdDate.toInstant()));
    }

    UUID updatedBy = row.getUUID("updated_by_user_id");
    if (nonNull(updatedBy)) {
      set.withUpdatedByUserId(updatedBy.toString());
    }

    OffsetDateTime updatedDate = row.getOffsetDateTime("updated_date");
    if (nonNull(updatedDate)) {
      set.withUpdatedDate(from(updatedDate.toInstant()));
    }

    return set;
  }

  private enum InsertType {
    INSERT, UPDATE
  }

  private String fkListToJsonString(List<FilteringCondition> filteringConditions) {
    JsonArray jsonArray = new JsonArray();
    filteringConditions.stream()
        .map(this::fkToJsonObject)
        .forEach(jsonArray::add);
    return jsonArray.toString();
  }

  private JsonObject fkToJsonObject(FilteringCondition filteringCondition) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.put("name", filteringCondition.getName());
    jsonObject.put("value", filteringCondition.getValue());
    jsonObject.put("setSpec", filteringCondition.getSetSpec());
    return jsonObject;
  }

  private List<FilteringCondition> jsonStringToFkList(String json) {
    return new JsonArray(json).stream()
        .map(JsonObject.class::cast)
        .map(this::jsonObjectToFilteringCondition)
        .toList();
  }

  private FilteringCondition jsonObjectToFilteringCondition(JsonObject jsonObject) {
    return new FilteringCondition()
        .withName(jsonObject.getString("name"))
        .withValue(jsonObject.getString("value"))
        .withSetSpec(jsonObject.getString("setSpec"));
  }

  private record SqlAndParams(String sql, Tuple params) {}

  private static SqlAndParams toSql(Query query) {
    String namedSql = JOOQ.renderNamedParams(query);
    List<Object> bindValues = JOOQ.extractBindValues(query);
    StringBuilder sb = new StringBuilder();
    int paramIndex = 1;
    for (int i = 0; i < namedSql.length(); ) {
      char c = namedSql.charAt(i);
      if (c == ':' && i + 1 < namedSql.length() && Character.isDigit(namedSql.charAt(i + 1))) {
        int j = i + 1;
        while (j < namedSql.length() && Character.isDigit(namedSql.charAt(j))) {
          j++;
        }
        sb.append('$').append(paramIndex++);
        i = j;
      } else {
        sb.append(c);
        i++;
      }
    }

    Tuple tuple = Tuple.tuple();
    bindValues.forEach(tuple::addValue);

    return new SqlAndParams(sb.toString(), tuple);
  }
}