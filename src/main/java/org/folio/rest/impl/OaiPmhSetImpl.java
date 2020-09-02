package org.folio.rest.impl;

import static java.lang.String.format;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.oaipmh.Constants.SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.dataimport.util.ExceptionHelper;
import org.folio.oaipmh.service.SetService;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.FolioSet;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.resource.OaiPmhFilteringConditions;
import org.folio.rest.jaxrs.resource.OaiPmhSets;
import org.folio.rest.persist.PgExceptionUtil;
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
import io.vertx.pgclient.PgException;

public class OaiPmhSetImpl implements OaiPmhSets, OaiPmhFilteringConditions {

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
          .map(OaiPmhSets.GetOaiPmhSetsByIdResponse::respond200WithApplicationJson)
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
  public void putOaiPmhSetsById(String id, String lang, FolioSet entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        logger.info("Put set by id with id: '{}' and body: {}", id, entity);
        validateFolioSet(entity, asyncResultHandler);
        setService.updateSetById(id, entity, getTenantId(okapiHeaders), getUserId(okapiHeaders))
          .map(updated -> OaiPmhSets.PutOaiPmhSetsByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(this::handleException)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error("Error occurred while putting set with id: {}. Message: {}. Exception: {}", id, e.getMessage(), e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postOaiPmhSets(String lang, FolioSet entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        logger.info("Post set with body: {}", entity);
        validateFolioSet(entity, asyncResultHandler);
        setService.saveSet(entity, getTenantId(okapiHeaders), getUserId(okapiHeaders))
          .map(set -> OaiPmhSets.PostOaiPmhSetsResponse.respond201WithApplicationJson(set, PostOaiPmhSetsResponse.headersFor201()))
          .map(Response.class::cast)
          .otherwise(this::handleException)
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
          .map(OaiPmhSets.DeleteOaiPmhSetsByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(this::handleException)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error("Error occurred while deleting set with id: '{}'. Message: {}. Exception: {}", id, e.getMessage(), e);
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

  @Override
  public void getOaiPmhFilteringConditions(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        setService.getFilteringConditions(okapiHeaders)
          .map(OaiPmhFilteringConditions.GetOaiPmhFilteringConditionsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  private void validateFolioSet(FolioSet folioSet, Handler<AsyncResult<Response>> asyncResultHandler) {
    List<Error> errorsList = new ArrayList<>();
    if (isEmpty(folioSet.getName())) {
      String message = format(SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE, "name");
      errorsList.add(createError("name", folioSet.getName(), message, ERROR_TYPE.EMPTY));
    }
    if (isEmpty(folioSet.getSetSpec())) {
      String message = format(SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE, "setSpec");
      errorsList.add(createError("setSpec", folioSet.getSetSpec(), message, ERROR_TYPE.EMPTY));
    }
    if (isNotEmpty(errorsList)) {
      Errors errors = new Errors();
      errors.setErrors(errorsList);
      asyncResultHandler.handle(Future.succeededFuture(PostOaiPmhSetsResponse.respond422WithApplicationJson(errors)));
    }
  }

  private Error createError(String field, String value, String message, ERROR_TYPE errorType) {
    Error error = new Error();
    Parameter p = new Parameter();
    p.setKey(field);
    p.setValue(value);
    error.getParameters()
      .add(p);
    error.setMessage(message);
    error.setCode(String.valueOf(errorType.ordinal()));
    error.setType(errorType.toString());
    return error;
  }

  private Response handleException(Throwable throwable) {
    Response response = null;
    if (throwable instanceof IllegalArgumentException) {
      response = OaiPmhSets.PostOaiPmhSetsResponse.respond400WithTextPlain(throwable.getMessage());
    } else if (throwable instanceof PgException) {
      Map<Character, String> pgErrorsMap = PgExceptionUtil.getBadRequestFields(throwable);
      int errorCode = Integer.parseInt(pgErrorsMap.get('C'));
      if (errorCode == 23505) {
        String fieldName = getFieldName(pgErrorsMap);
        String fieldValue = getFieldValue(pgErrorsMap);
        String errorMessage = format(
            "Field '%s' cannot have duplicated values. Value '%s' is already taken. Please, pass another value", fieldName,
            fieldValue);
        Error error = createError(fieldName, fieldValue, errorMessage, ERROR_TYPE.NOT_UNIQUE);
        Errors errors = new Errors().withErrors(Collections.singletonList(error));
        response = PostOaiPmhSetsResponse.respond422WithApplicationJson(errors);
      }
    } else {
      response = ExceptionHelper.mapExceptionToResponse(throwable);
    }
    return response;
  }

  private String getFieldName(Map<Character, String> pgErrorsMap) {
    String error = pgErrorsMap.get('D');
    String val = (error.split("\\)")[0].split("\\(")[1]);
    if (val.contains("set_spec")) {
      return val.replaceAll("_spec", "Spec");
    }
    return val;
  }

  private String getFieldValue(Map<Character, String> pgErrorsMap) {
    String error = pgErrorsMap.get('D');
    String value = error.split("=")[1].split("\\)")[0];
    return value.substring(1);
  }

  private String getTenantId(Map<String, String> okapiHeaders) {
    return TenantTool.tenantId(okapiHeaders);
  }

  private String getUserId(Map<String, String> okapiHeaders) {
    return okapiHeaders.get("x-okapi-user-id");
  }

  private enum ERROR_TYPE {
    EMPTY, NOT_UNIQUE {
      @Override
      public String toString() {
        return "notUnique";
      }
    };

    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

}
