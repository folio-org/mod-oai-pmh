package org.folio.rest.impl;

import static java.lang.String.format;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.oaipmh.Constants.SET_FIELD_NULL_VALUE_ERROR_MSG_TEMPLATE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.dataimport.util.ExceptionHelper;
import org.folio.oaipmh.service.SetService;
import org.folio.okapi.common.XOkapiHeaders;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import io.vertx.pgclient.PgException;

public class OaiPmhSetImpl implements OaiPmhSets, OaiPmhFilteringConditions {

  private static final Logger logger = LogManager.getLogger(OaiPmhSetImpl.class);
  private static final String MANAGE_SET_BY_ID_ERROR_MESSAGE_TEMPLATE = "Error occurred while {} set with id: {}. Message: {}.";
  private static final String POST_SET_ERROR_MESSAGE = "Error occurred while posting set with body: {}. Message: {}.";
  private static final String GET_SET_LIST_ERROR_MESSAGE = "Error occurred while getting list of sets with offset: {} and limit: {}. Message: {}.";

  @Autowired
  private SetService setService;

  public OaiPmhSetImpl() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void getOaiPmhSetsById(String id, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        logger.info("Get set by id with id: {}.", id);
        setService.getSetById(id, getTenantId(okapiHeaders))
          .map(OaiPmhSets.GetOaiPmhSetsByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error(MANAGE_SET_BY_ID_ERROR_MESSAGE_TEMPLATE, "getting", id, e.getMessage(), e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putOaiPmhSetsById(String id, FolioSet entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        logger.info("Put set by id with id: '{}' and body: {}", id, entityToJsonString(entity));
        validateFolioSet(entity, asyncResultHandler);
        setService.updateSetById(id, entity, getTenantId(okapiHeaders), getUserId(okapiHeaders))
          .map(updated -> OaiPmhSets.PutOaiPmhSetsByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(this::handleException)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error(MANAGE_SET_BY_ID_ERROR_MESSAGE_TEMPLATE, "putting", id, e.getMessage(), e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postOaiPmhSets(FolioSet entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        logger.info("Post set with body: {}.", entityToJsonString(entity));
        validateFolioSet(entity, asyncResultHandler);
        setService.saveSet(entity, getTenantId(okapiHeaders), getUserId(okapiHeaders))
          .map(set -> OaiPmhSets.PostOaiPmhSetsResponse.respond201WithApplicationJson(set, PostOaiPmhSetsResponse.headersFor201()))
          .map(Response.class::cast)
          .otherwise(this::handleException)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error(POST_SET_ERROR_MESSAGE, entity, e.getMessage(), e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteOaiPmhSetsById(String id, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        logger.info("Delete set by id '{}'.", id);
        setService.deleteSetById(id, getTenantId(okapiHeaders))
          .map(OaiPmhSets.DeleteOaiPmhSetsByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(this::handleException)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error(MANAGE_SET_BY_ID_ERROR_MESSAGE_TEMPLATE,"deleting", id, e.getMessage(), e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getOaiPmhSets(String totalRecords, int offset, int limit, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    vertxContext.runOnContext(v -> {
      try {
        logger.info("Get list of sets, offset: {}, limit: {}.", offset, limit);
        setService.getSetList(offset, limit, getTenantId(okapiHeaders))
          .map(GetOaiPmhSetsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        logger.error(GET_SET_LIST_ERROR_MESSAGE, offset, limit, e.getMessage(), e);
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
    String val = error.split("\\(")[2].replace("::text))=", "");
    if (val.contains("set_spec")) {
      return val.replace("_spec", "Spec");
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
    return okapiHeaders.get(XOkapiHeaders.USER_ID);
  }

  private String entityToJsonString(FolioSet folioSet) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(folioSet);
    } catch (IOException ex) {
      logger.error("Cannot transform dto object to json string for entity logging.");
      return folioSet.toString();
    }
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
