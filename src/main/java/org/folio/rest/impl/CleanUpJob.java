package org.folio.rest.impl;

import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.rest.jaxrs.resource.OaiPmhCleanUpInstances.PostOaiPmhCleanUpInstancesResponse.respond500WithTextPlain;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.service.InstancesService;
import org.folio.rest.jaxrs.resource.OaiPmhCleanUpInstances;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

public class CleanUpJob implements OaiPmhCleanUpInstances {

  private final Logger logger = LogManager.getLogger(this.getClass());

  private static final long INSTANCES_EXPIRATION_TIME_IN_SECONDS = TimeUnit.DAYS.toSeconds(30);

  private InstancesService instancesService;

  public CleanUpJob() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void postOaiPmhCleanUpInstances(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    logger.debug("Running instances clean up job.");
    vertxContext.runOnContext(v -> instancesService.cleanExpiredInstances(
        okapiHeaders.get(OKAPI_TENANT), INSTANCES_EXPIRATION_TIME_IN_SECONDS)
        .map(PostOaiPmhCleanUpInstancesResponse.respond204())
        .map(Response.class::cast)
        .otherwise(throwable -> respond500WithTextPlain(throwable.getMessage()))
        .onComplete(asyncResultHandler));
  }

  @Autowired
  public void setInstancesService(InstancesService instancesService) {
    this.instancesService = instancesService;
  }
}
