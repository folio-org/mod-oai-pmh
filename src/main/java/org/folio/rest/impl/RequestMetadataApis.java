package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.io.IOException;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.rest.jaxrs.resource.OaiRequestMetadata;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.s3.client.FolioS3Client;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

public class RequestMetadataApis implements OaiRequestMetadata {

  private static final Logger logger = LogManager.getLogger(RequestMetadataApis.class);
  private static final String REQUEST_METADATA_ERROR_MESSAGE_TEMPLATE =
      "Error occurred while get request metadata. Message: {}.";
  private static final String UUID_COLLECTION_ERROR_MESSAGE_TEMPLATE =
      "Error occurred while get UUIDs collection. Message: {}.";
  private static final String DOWNLOAD_LOG_ERROR_MESSAGE_TEMPLATE =
      "Error occurred while downloading log. Message: {}.";


  @Autowired
  InstancesDao instancesDao;

  @Autowired
  FolioS3Client folioS3Client;

  public RequestMetadataApis() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void getOaiRequestMetadata(String totalRecords, int offset, int limit,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        var tenantId = TenantTool.tenantId(okapiHeaders);
        logger.info("Get request metadata collection for tenant: {}", tenantId);
        instancesDao.getRequestMetadataCollection(offset, limit, tenantId)
            .map(GetOaiRequestMetadataResponse::respond200WithApplicationJson)
            .map(Response.class::cast)
            .otherwise(ExceptionHelper::mapExceptionToResponse)
            .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error(REQUEST_METADATA_ERROR_MESSAGE_TEMPLATE, e.getMessage());
        asyncResultHandler.handle(Future.succeededFuture(
            ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getOaiRequestMetadataFailedToSaveInstancesByRequestId(String requestId,
      String totalRecords, int offset, int limit, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      var tenantId = TenantTool.tenantId(okapiHeaders);
      logger.info("Get failed to save instances UUIDs collection for tenant: {}, requestId: {}",
          tenantId, requestId);
      instancesDao.getFailedToSaveInstancesIdsCollection(requestId, offset, limit, tenantId)
        .map(GetOaiRequestMetadataFailedToSaveInstancesByRequestIdResponse
            ::respond200WithApplicationJson)
        .map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
    } catch (Exception e) {
      logger.error(UUID_COLLECTION_ERROR_MESSAGE_TEMPLATE, e.getMessage());
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void getOaiRequestMetadataSkippedInstancesByRequestId(String requestId,
      String totalRecords, int offset, int limit, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      var tenantId = TenantTool.tenantId(okapiHeaders);
      logger.info("Get skipped instances UUIDs collection for tenant: {}, requestId: {}",
          tenantId, requestId);
      instancesDao.getSkippedInstancesIdsCollection(requestId, offset, limit, tenantId)
          .map(GetOaiRequestMetadataSkippedInstancesByRequestIdResponse
              ::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
    } catch (Exception e) {
      logger.error(UUID_COLLECTION_ERROR_MESSAGE_TEMPLATE, e.getMessage());
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void getOaiRequestMetadataFailedInstancesByRequestId(String requestId,
      String totalRecords, int offset, int limit, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      var tenantId = TenantTool.tenantId(okapiHeaders);
      logger.info("Get failed UUIDs collection for tenant: {}, requestId: {}",
          tenantId, requestId);
      instancesDao.getFailedInstancesIdsCollection(requestId, offset, limit, tenantId)
          .map(GetOaiRequestMetadataFailedInstancesByRequestIdResponse
              ::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
    } catch (Exception e) {
      logger.error(UUID_COLLECTION_ERROR_MESSAGE_TEMPLATE, e.getMessage());
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void getOaiRequestMetadataSuppressedFromDiscoveryInstancesByRequestId(String requestId,
      String totalRecords, int offset, int limit, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      var tenantId = TenantTool.tenantId(okapiHeaders);
      logger.info("Get suppressed from discovery instances UUIDs collection for "
          + "tenant: {}, requestId: {}", tenantId, requestId);
      instancesDao.getSuppressedInstancesIdsCollection(requestId, offset, limit, tenantId)
          .map(GetOaiRequestMetadataSuppressedFromDiscoveryInstancesByRequestIdResponse
              ::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
    } catch (Exception e) {
      logger.error(UUID_COLLECTION_ERROR_MESSAGE_TEMPLATE, e.getMessage());
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void getOaiRequestMetadataLogsByRequestId(String requestId, Map<String,
      String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        var tenantId = TenantTool.tenantId(okapiHeaders);
        logger.info("Download error log for tenant: {}, requestId: {}",
            tenantId, requestId);
        instancesDao.getRequestMetadataByRequestId(requestId, tenantId)
            .map(RequestMetadataLb::getPathToErrorFileInS3)
            .map(this::prepareResponseData)
            .map(Response.class::cast)
            .otherwise(ExceptionHelper::mapExceptionToResponse)
            .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error(DOWNLOAD_LOG_ERROR_MESSAGE_TEMPLATE, e.getMessage());
        asyncResultHandler.handle(Future.succeededFuture(
            ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  private GetOaiRequestMetadataLogsByRequestIdResponse prepareResponseData(
      String pathToErrorFile) {
    var response = GetOaiRequestMetadataLogsByRequestIdResponse.respond200WithBinaryOctetStream(
        new String(readFile(pathToErrorFile)));
    var headers = response.getHeaders();
    headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + pathToErrorFile);
    return response;
  }

  private byte[] readFile(String path) {
    try (var is = folioS3Client.read(path)) {
      return is.readAllBytes();
    } catch (IOException e) {
      return new byte[0];
    }
  }
}
