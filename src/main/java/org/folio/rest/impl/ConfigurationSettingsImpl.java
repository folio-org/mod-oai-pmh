package org.folio.rest.impl;

import static java.lang.String.format;

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
import org.folio.oaipmh.service.ConfigurationSettingsService;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.ConfigurationSettings;
import org.folio.rest.jaxrs.resource.OaiPmhConfigurationSettings;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

public class ConfigurationSettingsImpl implements OaiPmhConfigurationSettings {

  private static final Logger logger = LogManager.getLogger(ConfigurationSettingsImpl.class);
  private static final String ERROR_MSG_TEMPLATE =
        "ConfigurationSettings with id '%s' was not found";

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
        .onFailure(throwable -> handleGenericFailure(throwable,
            "Failed to retrieve configuration settings", asyncResultHandler));
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
        .onFailure(throwable -> handleCreateFailure(throwable, asyncResultHandler));
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
        .onFailure(throwable -> handleFailureWithNotFound(throwable, id,
            "Failed to retrieve configuration setting by id: " + id,
            "Failed to retrieve configuration setting", asyncResultHandler));
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
        .onFailure(throwable -> handleFailureWithNotFound(throwable, id,
            "Failed to update configuration setting by id: " + id,
            "Failed to update configuration setting", asyncResultHandler));
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
        .onFailure(throwable -> handleFailureWithNotFound(throwable, id,
            "Failed to delete configuration setting by id: " + id,
            "Failed to delete configuration setting", asyncResultHandler));
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
        .onFailure(throwable -> {
          logger.error("Failed to retrieve configuration setting by name: {}",
              configName, throwable);
          if (throwable instanceof javax.ws.rs.NotFoundException) {
            asyncResultHandler.handle(Future.succeededFuture(
                Response.status(Response.Status.NOT_FOUND)
                    .entity("Configuration setting with name '" + configName + "' was not found")
                  .build()));
          } else {
            asyncResultHandler.handle(Future.succeededFuture(
                Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Failed to retrieve configuration setting: "
                      + throwable.getMessage()).build()));
          }
        });
  }

  private void handleGenericFailure(Throwable throwable, String errorMessage,
      Handler<AsyncResult<Response>> asyncResultHandler) {
    asyncResultHandler.handle(Future.succeededFuture(
        Response.status(Response.Status.INTERNAL_SERVER_ERROR)
            .entity(errorMessage + ": " + throwable.getMessage()).build()));
  }

  private void handleCreateFailure(Throwable throwable,
      Handler<AsyncResult<Response>> asyncResultHandler) {
    logger.error("Failed to create configuration setting", throwable);
    if (throwable instanceof IllegalArgumentException) {
      asyncResultHandler.handle(Future.succeededFuture(
          Response.status(Response.Status.BAD_REQUEST)
              .entity(throwable.getMessage()).build()));
    } else {
      asyncResultHandler.handle(Future.succeededFuture(
          Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity("Failed to create configuration setting: "
                + throwable.getMessage()).build()));
    }
  }

  private void handleFailureWithNotFound(Throwable throwable, String id, String logMessage,
      String errorPrefix, Handler<AsyncResult<Response>> asyncResultHandler) {
    logger.error(logMessage, throwable);
    if (throwable instanceof javax.ws.rs.NotFoundException) {
      asyncResultHandler.handle(Future.succeededFuture(
          Response.status(Response.Status.NOT_FOUND)
              .entity(format(ERROR_MSG_TEMPLATE, id)).build()));
    } else {
      asyncResultHandler.handle(Future.succeededFuture(
          Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity(errorPrefix + ": " + throwable.getMessage()).build()));
    }
  }
}
