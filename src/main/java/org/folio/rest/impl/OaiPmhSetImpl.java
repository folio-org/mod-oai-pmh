package org.folio.rest.impl;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.dataimport.util.ExceptionHelper;
import org.folio.oaipmh.service.SetService;
import org.folio.rest.jaxrs.model.Set;
import org.folio.rest.jaxrs.resource.OaiPmhSets;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class OaiPmhSetImpl implements OaiPmhSets {

  private static final Logger logger = LoggerFactory.getLogger(OaiPmhSetImpl.class);

  @Autowired
  private SetService setService;

  public OaiPmhSetImpl() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void getOaiPmhSetsById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        logger.info("Get set by id with id: '{}'", id);
        setService.getSetById(id, getTenantId(okapiHeaders))
          .map(GetOaiPmhSetsByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error("Error occurred while getting set with id: {}. Message: {}. Exception: {}", id, e.getMessage(), e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putOaiPmhSetsById(String id, String lang, Set entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        logger.info("Put set by id with id: '{}' and body: {}", id, entity);
        setService.updateSetById(id, entity, getTenantId(okapiHeaders), getUserId(okapiHeaders))
          .map(updated -> PutOaiPmhSetsByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error("Error occurred while putting set with id: {}. Message: {}. Exception: {}", id, e.getMessage(), e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postOaiPmhSets(String lang, Set entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        logger.info("Post set with body: {}", entity);
        setService.saveSet(entity, getTenantId(okapiHeaders), getUserId(okapiHeaders))
          .map(set -> PostOaiPmhSetsResponse.respond201WithApplicationJson(set, PostOaiPmhSetsResponse.headersFor201()))
          .map(Response.class::cast)
          .otherwise(throwable -> {
            if (throwable instanceof IllegalArgumentException) {
              return PostOaiPmhSetsResponse.respond400WithTextPlain(throwable.getMessage());
            }
            return ExceptionHelper.mapExceptionToResponse(throwable);
          })
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error("Error occurred while posting set with body: {}. Message: {}. Exception: {}", entity, e.getMessage(), e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteOaiPmhSetsById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        logger.info("Delete set by id with id: '{}'", id);
        setService.deleteSetById(id, getTenantId(okapiHeaders))
          .map(DeleteOaiPmhSetsByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error("Error occurred while posting set with id: '{}'. Message: {}. Exception: {}", id, e.getMessage(), e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getOaiPmhSets(int offset, int limit, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        logger.info("Get list of sets, offset: '{}', limit: '{}'", offset, limit);
        setService.getSetList(offset, limit, getTenantId(okapiHeaders))
          .map(GetOaiPmhSetsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error("Error occurred while getting list of sets with offset: '{}' and limit: '{}'. Message: {}. Exception: {}",
            offset, limit, e.getMessage(), e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  private String getTenantId(Map<String, String> okapiHeaders) {
    return TenantTool.tenantId(okapiHeaders);
  }

  private String getUserId(Map<String, String> okapiHeaders) {
    return okapiHeaders.get("x-okapi-user-id");
  }

}
