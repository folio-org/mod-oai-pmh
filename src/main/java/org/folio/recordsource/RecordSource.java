package org.folio.recordsource;

import java.util.List;

import javax.ws.rs.core.Response;

import io.vertx.core.Future;

/**
 * Abstraction over the location of instances, holdings and items and the way of getting them
 */
//TODO
public interface RecordSource {

  Future<Response> getAll();

  void streamGet();

  Response getByIds(List<String> ids);

  Response getById(String id);

}
