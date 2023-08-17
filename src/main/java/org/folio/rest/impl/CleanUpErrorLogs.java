package org.folio.rest.impl;

import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.REPOSITORY_FETCHING_CLEAN_ERRORS_INTERVAL;
import static org.folio.rest.jaxrs.resource.OaiPmhCleanUpInstances.PostOaiPmhCleanUpInstancesResponse.respond500WithTextPlain;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.service.ErrorsService;
import org.folio.oaipmh.service.InstancesService;
import org.folio.rest.jaxrs.resource.OaiPmhCleanUpErrorLogs;
import org.folio.rest.jaxrs.resource.OaiPmhCleanUpInstances;
import org.folio.s3.client.FolioS3Client;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

public class CleanUpErrorLogs implements OaiPmhCleanUpErrorLogs {

  private final Logger logger = LogManager.getLogger(this.getClass());

  @Autowired
  private FolioS3Client folioS3Client;

  @Autowired
  private InstancesService instancesService;

  @Autowired
  private ErrorsService errorsService;

  public CleanUpErrorLogs() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void postOaiPmhCleanUpErrorLogs(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    logger.debug("Running cleaning up error logs");
    long interval = Long.parseLong(System.getProperty(REPOSITORY_FETCHING_CLEAN_ERRORS_INTERVAL));

    OffsetDateTime offsetDateTime = ZonedDateTime
      .ofInstant(Instant.now(), ZoneId.of("UTC"))
      .minusDays(interval)
      .toOffsetDateTime();

    var tenant = okapiHeaders.get(OKAPI_TENANT);

    vertxContext.runOnContext(v ->
      instancesService.getRequestMetadataIdsByStartedDateAndExistsByPathToErrorFileInS3(tenant, offsetDateTime)
        .onComplete(result -> {
          if (result.succeeded()) {
            if (!result.result().isEmpty()) {
              result.result().forEach(id -> {
                try {
                  instancesService.updateRequestMetadataByPathToError(id, tenant, null);
                } catch (Exception ex) {
                  logger.error("Error occurred while updateRequestMetadataByPathToError : requestId: {}", id);
                }
                try {
                  instancesService.updateRequestMetadataByLinkToError(id, tenant, null);
                } catch (Exception ex) {
                  logger.error("Error occurred while updateRequestMetadataByLinkToError : requestId: {}", id);
                }
                try {
                  folioS3Client.remove(id + "-error.csv");
                } catch (Exception ex) {
                  logger.error("Error occurred while deleting file from S3: fileName: {}", id + "-error.csv");
                }
                try {
                  errorsService.deleteErrorsByRequestId(tenant, id);
                } catch (Exception ex) {
                  logger.error("Error occurred while deleteErrorsByRequestId: requestId: {}", id);
                }
              });
            } else {
              logger.debug("Nothing to clean (error logs)");
            }
          } else {
            logger.error("Error occurred while selecting getRequestMetadataIdsByStartedDateAndExistsByPathToErrorFileInS3");
          }
        })
        .map(OaiPmhCleanUpInstances.PostOaiPmhCleanUpInstancesResponse.respond204())
        .map(Response.class::cast)
        .otherwise(throwable -> respond500WithTextPlain(throwable.getMessage()))
        .onComplete(asyncResultHandler));
  }
}
