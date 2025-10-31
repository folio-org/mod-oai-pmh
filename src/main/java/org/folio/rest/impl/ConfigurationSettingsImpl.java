package org.folio.rest.impl;


import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.oaipmh.service.ConfigurationSettingsService;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.ConfigurationSettings;
import org.folio.rest.jaxrs.resource.OaiPmhConfigurationSettings;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

public class ConfigurationSettingsImpl implements OaiPmhConfigurationSettings {

  private static final Logger logger = LogManager.getLogger(ConfigurationSettingsImpl.class);

  private static final String ERROR_NOT_FOUND = "ConfigurationSettings was not found";

  private static final String ERROR_RETRIEVE = "Failed to retrieve configuration settings";

  private static final String ERROR_CREATE = "Failed to create configuration setting";

  private static final String ERROR_UPDATE = "Failed to update configuration setting";

  private static final String ERROR_DELETE = "Failed to delete configuration setting";


  @Autowired
  private ConfigurationSettingsService configurationSettingsService;

  public ConfigurationSettingsImpl() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  private record OkapiContext(String tenantId, String userId) {}

  private static OkapiContext extractOkapiContext(Map<String, String> headers) {
    return new OkapiContext(headers.get(XOkapiHeaders.TENANT), headers
      .get(XOkapiHeaders.USER_ID));
  }

  public void getOaiPmhConfigurationSettings(String totalRecords, int offset, int limit,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    String tenantId = extractOkapiContext(okapiHeaders).tenantId;

    configurationSettingsService.getConfigurationSettingsList(offset, limit, tenantId)
        .onSuccess(configSettings -> {
          asyncResultHandler.handle(Future.succeededFuture(
              Response.ok().entity(configSettings.encode()).build()));
        })
        .onFailure(e -> handleFailure(asyncResultHandler, e, ERROR_RETRIEVE));
  }

  public void postOaiPmhConfigurationSettings(ConfigurationSettings entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    OkapiContext ctx = extractOkapiContext(okapiHeaders);
    configurationSettingsService.saveConfigurationSettings(JsonObject.mapFrom(entity),
        ctx.tenantId, ctx.userId)
        .onSuccess(savedConfig -> {
          asyncResultHandler.handle(Future.succeededFuture(
              Response.status(Response.Status.CREATED).entity(savedConfig.encode()).build()));
        })
        .onFailure(e -> handleFailure(asyncResultHandler, e, ERROR_CREATE));
  }

  public void getOaiPmhConfigurationSettingsById(String id,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    String tenantId = extractOkapiContext(okapiHeaders).tenantId;

    configurationSettingsService.getConfigurationSettingsById(id, tenantId)
        .onSuccess(configSetting -> {
          asyncResultHandler.handle(Future.succeededFuture(
              Response.ok().entity(configSetting.encode()).build()));
        })
        .onFailure(e -> handleFailure(asyncResultHandler, e, ERROR_NOT_FOUND));
  }

  public void putOaiPmhConfigurationSettingsById(String id, ConfigurationSettings entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    OkapiContext ctx = extractOkapiContext(okapiHeaders);

    configurationSettingsService.updateConfigurationSettingsById(id,
        JsonObject.mapFrom(entity), ctx.tenantId, ctx.userId)
        .onSuccess(updatedConfig -> {
          asyncResultHandler.handle(Future.succeededFuture(
              Response.ok().entity(updatedConfig.encode()).build()));
        })
        .onFailure(e -> handleFailure(asyncResultHandler, e, ERROR_UPDATE));
  }

  public void deleteOaiPmhConfigurationSettingsById(String id,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    String tenantId = extractOkapiContext(okapiHeaders).tenantId;

    configurationSettingsService.deleteConfigurationSettingsById(id, tenantId)
        .onSuccess(deleted -> {
          asyncResultHandler.handle(Future.succeededFuture(
              Response.status(Response.Status.NO_CONTENT).build()));
        })
        .onFailure(e -> handleFailure(asyncResultHandler, e, ERROR_DELETE));
  }

  public void getOaiPmhConfigurationSettingsByName(String configName, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    String tenantId = extractOkapiContext(okapiHeaders).tenantId;
    getConfigurationByName(configName, tenantId, asyncResultHandler);
  }

  public void getOaiPmhConfigurationSettingsNameByConfigName(String configName,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    String tenantId = extractOkapiContext(okapiHeaders).tenantId;
    getConfigurationByName(configName, tenantId, asyncResultHandler);
  }

  private void getConfigurationByName(String configName, String tenantId,
      Handler<AsyncResult<Response>> asyncResultHandler) {

    configurationSettingsService.getConfigurationSettingsByName(configName, tenantId)
        .onSuccess(configSetting -> {
          asyncResultHandler.handle(Future.succeededFuture(
              Response.ok().entity(configSetting.encode()).build()));
        })
        .onFailure(e -> handleFailure(asyncResultHandler, e, ERROR_RETRIEVE));
  }

  private void handleFailure(Handler<AsyncResult<Response>> asyncResultHandler,
                             Throwable e, String errorMessage) {
    logger.error(errorMessage, e);
    asyncResultHandler.handle(Future.succeededFuture(
        ExceptionHelper.mapExceptionToResponse(e)
    ));
  }

}
