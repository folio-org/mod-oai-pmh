package org.folio.recordsource;

import java.util.List;

import javax.ws.rs.core.Response;

import org.folio.oaipmh.Request;

import io.vertx.core.Future;

public abstract class AbstractRecordSource implements RecordSource {

  protected final Request request;

  protected AbstractRecordSource(Request request) {
    this.request = request;
  }

  @Override
  public Future<Response> getAll() {
    return null;
  }

  @Override
  public void streamGet() {

  }

  @Override
  public Response getByIds(List<String> identifiers) {
    return null;
  }

  @Override
  public Response getById(String id) {
    return null;
  }
}
