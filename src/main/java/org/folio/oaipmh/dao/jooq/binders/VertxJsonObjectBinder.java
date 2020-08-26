package org.folio.oaipmh.dao.jooq.binders;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Objects;

import org.folio.oaipmh.dao.jooq.converters.PostgresJsonToVertxJsonObjectConverter;
import org.jooq.Binding;
import org.jooq.BindingGetResultSetContext;
import org.jooq.BindingGetSQLInputContext;
import org.jooq.BindingGetStatementContext;
import org.jooq.BindingRegisterContext;
import org.jooq.BindingSQLContext;
import org.jooq.BindingSetSQLOutputContext;
import org.jooq.BindingSetStatementContext;
import org.jooq.Converter;
import org.jooq.impl.DSL;

import io.vertx.core.json.JsonObject;

public class VertxJsonObjectBinder implements Binding<Object, JsonObject> {

  @Override
  public Converter<Object, JsonObject> converter() {
    return new PostgresJsonToVertxJsonObjectConverter();
  }

  @Override
  public void sql(BindingSQLContext<JsonObject> ctx) throws SQLException {
    ctx.render().visit(DSL.val(ctx.convert(converter()).value())).sql("::json");
  }

  @Override
  public void register(BindingRegisterContext<JsonObject> ctx) throws SQLException {
    ctx.statement().registerOutParameter(ctx.index(), Types.VARCHAR);
  }

  @Override
  public void set(BindingSetStatementContext<JsonObject> ctx) throws SQLException {
    ctx.statement().setString(
      ctx.index(),
      Objects.toString(ctx.convert(converter()).value()));
  }

  @Override
  public void get(BindingGetResultSetContext<JsonObject> ctx) throws SQLException {
    ctx.convert(converter()).value(ctx.resultSet().getString(ctx.index()));
  }

  @Override
  public void get(BindingGetStatementContext<JsonObject> ctx) throws SQLException {
    ctx.convert(converter()).value(ctx.statement().getString(ctx.index()));
  }

  @Override
  public void get(BindingGetSQLInputContext<JsonObject> bindingGetSQLInputContext) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void set(BindingSetSQLOutputContext<JsonObject> ctx) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
