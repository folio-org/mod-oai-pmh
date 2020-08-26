package org.folio.oaipmh.dao.jooq.converters;

import org.jooq.Converter;

import io.vertx.core.json.JsonObject;


public class PostgresJsonToVertxJsonObjectConverter implements Converter<Object, JsonObject> {

  @Override
  public JsonObject from(Object o) {
    return o == null ? new JsonObject() : new JsonObject(o.toString());
  }

  @Override
  public Object to(JsonObject jsonObject) {
    return jsonObject == null ? null : jsonObject.toString();
  }

  @Override
  public Class<Object> fromType() {
    return Object.class;
  }

  @Override
  public Class<JsonObject> toType() {
    return JsonObject.class;
  }
}
