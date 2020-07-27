package org.folio.rest.impl;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.dataimport.util.ExceptionHelper;
import org.folio.oaipmh.service.SetService;
import org.folio.rest.jaxrs.model.Set;
import org.folio.rest.jaxrs.resource.OaiPmhSet;
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

public class OaiPmhSetImpl implements OaiPmhSet {

  private static final Logger logger = LoggerFactory.getLogger(OaiPmhSetImpl.class);

  @Autowired
  private SetService setService;

  public OaiPmhSetImpl() {
    logger.info("OaiPmhSetImpl constructor start");
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    logger.info("OaiPmhSetImpl constructor finish");
  }

  @Override
  public void getOaiPmhSetById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        setService.getSetById(id, getTenantId(okapiHeaders))
          .map(GetOaiPmhSetByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putOaiPmhSetById(String id, String lang, Set entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        setService.updateSetById(id, entity, getTenantId(okapiHeaders), getUserId(okapiHeaders))
          .map(updated -> PutOaiPmhSetByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postOaiPmhSet(Set entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        setService.saveSet(entity, getTenantId(okapiHeaders), getUserId(okapiHeaders))
          .map(PostOaiPmhSetResponse::respond201WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(throwable -> {
            if(throwable instanceof IllegalArgumentException) {
              return PostOaiPmhSetResponse.respond400WithTextPlain(throwable.getMessage());
            }
            return ExceptionHelper.mapExceptionToResponse(throwable);
          })
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteOaiPmhSetById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        setService.deleteSetById(id, getTenantId(okapiHeaders))
          .map(DeleteOaiPmhSetByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
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
