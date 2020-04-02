package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.ws.rs.core.Response;

import java.util.Map;

import org.folio.rest.jaxrs.model.TenantAttributes;

public class TenantAPIs extends TenantAPI {
  private final Logger logger = LoggerFactory.getLogger(TenantAPIs.class);

  @Override
  public void postTenant(final TenantAttributes entity, final Map<String, String> headers,
      final Handler<AsyncResult<Response>> handlers, final Context context) {
    logger.debug("@@@@@@@@@@@@@@@@@@@@@@@@@ POST @@@@@@@@@@@@@@@@@@@@@@@@@");
    logTenantData(headers);
  }

  private void logTenantData(final Map<String, String> headers) {
    StringBuilder stringBuilder = new StringBuilder("====================TENANT API====================")
      .append(System.lineSeparator());

    headers.forEach((key, value) -> {
      addHeaderKeyValue(stringBuilder, key, value);
    });
    logger.debug(stringBuilder.toString());
  }

  @Override
  public void deleteTenant(final Map<String, String> headers, final Handler<AsyncResult<Response>> handlers, final Context context) {
    logger.debug("@@@@@@@@@@@@@@@@@@@@@@@@@ DELETE @@@@@@@@@@@@@@@@@@@@@@@@@");
    logTenantData(headers);
  }

  private void addHeaderKeyValue(StringBuilder stringBuilder, String key, String value) {
    stringBuilder.append("KEY : ")
      .append(key)
      .append(" ")
      .append("VALUE : ")
      .append(value)
      .append(System.lineSeparator());
  }
}
