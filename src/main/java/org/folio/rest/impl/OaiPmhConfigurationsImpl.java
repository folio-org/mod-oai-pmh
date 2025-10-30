package org.folio.rest.impl;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.service.ConfigurationCrudService;
import org.folio.rest.jaxrs.model.Configuration;
import org.folio.rest.jaxrs.model.ConfigurationCollection;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.resource.OaiPmhConfigurations;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

public class OaiPmhConfigurationsImpl implements OaiPmhConfigurations {

  private static final Logger logger = LogManager.getLogger(OaiPmhConfigurationsImpl.class);
  private static final String CONFIGURATION_NOT_FOUND_ERROR = "Configuration not found";
  private static final String CONFIGURATION_ERROR_MESSAGE_TEMPLATE = 
      "Error occurred while {} configuration. Message: {}.";
  private static final String CONFIGURATION_BY_ID_ERROR_MESSAGE_TEMPLATE = 
      "Error occurred while {} configuration with id: {}. Message: {}.";

  @Autowired
  private ConfigurationCrudService configurationCrudService;

  public OaiPmhConfigurationsImpl() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void getOaiPmhConfigurations(String configName, boolean enabled, String query,
      String totalRecords, int offset, int limit, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    
    vertxContext.runOnContext(v -> {
      try {
        String tenantId = TenantTool.tenantId(okapiHeaders);
        logger.info("Getting configurations for tenant: {}, configName: {}, enabled: {}, query: {}, offset: {}, limit: {}", 
                   tenantId, configName, enabled, query, offset, limit);

        configurationCrudService.getConfigurations(tenantId, configName, enabled, query, offset, limit)
          .onSuccess(configurations -> {
            ConfigurationCollection collection = new ConfigurationCollection()
                .withConfigurations(configurations)
                .withTotalRecords(configurations.size());
            
            asyncResultHandler.handle(Future.succeededFuture(
                GetOaiPmhConfigurationsResponse.respond200WithApplicationJson(collection)));
          })
          .onFailure(throwable -> {
            logger.error(format(CONFIGURATION_ERROR_MESSAGE_TEMPLATE, "getting", throwable.getMessage()), throwable);
            asyncResultHandler.handle(Future.succeededFuture(
                GetOaiPmhConfigurationsResponse.respond500WithApplicationJson(
                    createErrorResponse(throwable.getMessage()))));
          });
      } catch (Exception e) {
        logger.error(format(CONFIGURATION_ERROR_MESSAGE_TEMPLATE, "getting", e.getMessage()), e);
        asyncResultHandler.handle(Future.succeededFuture(
            GetOaiPmhConfigurationsResponse.respond500WithApplicationJson(
                createErrorResponse(e.getMessage()))));
      }
    });
  }

  @Override
  public void postOaiPmhConfigurations(Configuration entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    
    vertxContext.runOnContext(v -> {
      try {
        String tenantId = TenantTool.tenantId(okapiHeaders);
        
        // Generate ID if not provided
        if (isEmpty(entity.getId())) {
          entity.setId(UUID.randomUUID().toString());
        }
        
        logger.info("Creating configuration with id: {} for tenant: {}", entity.getId(), tenantId);

        configurationCrudService.createConfiguration(entity, tenantId)
          .onSuccess(createdConfiguration -> {
            logger.info("Successfully created configuration with id: {}", createdConfiguration.getId());
            asyncResultHandler.handle(Future.succeededFuture(
                PostOaiPmhConfigurationsResponse.respond201WithApplicationJson(createdConfiguration)));
          })
          .onFailure(throwable -> {
            logger.error(format(CONFIGURATION_ERROR_MESSAGE_TEMPLATE, "creating", throwable.getMessage()), throwable);
            if (throwable.getMessage().contains("duplicate") || throwable.getMessage().contains("already exists")) {
              asyncResultHandler.handle(Future.succeededFuture(
                  PostOaiPmhConfigurationsResponse.respond422WithApplicationJson(
                      createErrorResponse("Configuration with this name already exists"))));
            } else {
              asyncResultHandler.handle(Future.succeededFuture(
                  PostOaiPmhConfigurationsResponse.respond500WithApplicationJson(
                      createErrorResponse(throwable.getMessage()))));
            }
          });
      } catch (Exception e) {
        logger.error(format(CONFIGURATION_ERROR_MESSAGE_TEMPLATE, "creating", e.getMessage()), e);
        asyncResultHandler.handle(Future.succeededFuture(
            PostOaiPmhConfigurationsResponse.respond500WithApplicationJson(
                createErrorResponse(e.getMessage()))));
      }
    });
  }

  @Override
  public void getOaiPmhConfigurationsByConfigurationId(String configurationId,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    
    vertxContext.runOnContext(v -> {
      try {
        String tenantId = TenantTool.tenantId(okapiHeaders);
        logger.info("Getting configuration by id: {} for tenant: {}", configurationId, tenantId);

        configurationCrudService.getConfigurationById(configurationId, tenantId)
          .onSuccess(configuration -> {
            if (configuration != null) {
              asyncResultHandler.handle(Future.succeededFuture(
                  GetOaiPmhConfigurationsByConfigurationIdResponse.respond200WithApplicationJson(configuration)));
            } else {
              asyncResultHandler.handle(Future.succeededFuture(
                  GetOaiPmhConfigurationsByConfigurationIdResponse.respond404WithTextPlain(CONFIGURATION_NOT_FOUND_ERROR)));
            }
          })
          .onFailure(throwable -> {
            logger.error(format(CONFIGURATION_BY_ID_ERROR_MESSAGE_TEMPLATE, "getting", configurationId, throwable.getMessage()), throwable);
            asyncResultHandler.handle(Future.succeededFuture(
                GetOaiPmhConfigurationsByConfigurationIdResponse.respond500WithApplicationJson(
                    createErrorResponse(throwable.getMessage()))));
          });
      } catch (Exception e) {
        logger.error(format(CONFIGURATION_BY_ID_ERROR_MESSAGE_TEMPLATE, "getting", configurationId, e.getMessage()), e);
        asyncResultHandler.handle(Future.succeededFuture(
            GetOaiPmhConfigurationsByConfigurationIdResponse.respond500WithApplicationJson(
                createErrorResponse(e.getMessage()))));
      }
    });
  }

  @Override
  public void putOaiPmhConfigurationsByConfigurationId(String configurationId, Configuration entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    
    vertxContext.runOnContext(v -> {
      try {
        String tenantId = TenantTool.tenantId(okapiHeaders);
        
        // Ensure the entity ID matches the path parameter
        entity.setId(configurationId);
        
        logger.info("Updating configuration with id: {} for tenant: {}", configurationId, tenantId);

        configurationCrudService.updateConfiguration(entity, tenantId)
          .onSuccess(updatedConfiguration -> {
            if (updatedConfiguration != null) {
              logger.info("Successfully updated configuration with id: {}", configurationId);
              asyncResultHandler.handle(Future.succeededFuture(
                  PutOaiPmhConfigurationsByConfigurationIdResponse.respond204()));
            } else {
              asyncResultHandler.handle(Future.succeededFuture(
                  PutOaiPmhConfigurationsByConfigurationIdResponse.respond404WithTextPlain(CONFIGURATION_NOT_FOUND_ERROR)));
            }
          })
          .onFailure(throwable -> {
            logger.error(format(CONFIGURATION_BY_ID_ERROR_MESSAGE_TEMPLATE, "updating", configurationId, throwable.getMessage()), throwable);
            asyncResultHandler.handle(Future.succeededFuture(
                PutOaiPmhConfigurationsByConfigurationIdResponse.respond500WithApplicationJson(
                    createErrorResponse(throwable.getMessage()))));
          });
      } catch (Exception e) {
        logger.error(format(CONFIGURATION_BY_ID_ERROR_MESSAGE_TEMPLATE, "updating", configurationId, e.getMessage()), e);
        asyncResultHandler.handle(Future.succeededFuture(
            PutOaiPmhConfigurationsByConfigurationIdResponse.respond500WithApplicationJson(
                createErrorResponse(e.getMessage()))));
      }
    });
  }

  @Override
  public void deleteOaiPmhConfigurationsByConfigurationId(String configurationId,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    
    vertxContext.runOnContext(v -> {
      try {
        String tenantId = TenantTool.tenantId(okapiHeaders);
        logger.info("Deleting configuration with id: {} for tenant: {}", configurationId, tenantId);

        configurationCrudService.deleteConfiguration(configurationId, tenantId)
          .onSuccess(deleted -> {
            if (deleted) {
              logger.info("Successfully deleted configuration with id: {}", configurationId);
              asyncResultHandler.handle(Future.succeededFuture(
                  DeleteOaiPmhConfigurationsByConfigurationIdResponse.respond204()));
            } else {
              asyncResultHandler.handle(Future.succeededFuture(
                  DeleteOaiPmhConfigurationsByConfigurationIdResponse.respond404WithTextPlain(CONFIGURATION_NOT_FOUND_ERROR)));
            }
          })
          .onFailure(throwable -> {
            logger.error(format(CONFIGURATION_BY_ID_ERROR_MESSAGE_TEMPLATE, "deleting", configurationId, throwable.getMessage()), throwable);
            asyncResultHandler.handle(Future.succeededFuture(
                DeleteOaiPmhConfigurationsByConfigurationIdResponse.respond500WithApplicationJson(
                    createErrorResponse(throwable.getMessage()))));
          });
      } catch (Exception e) {
        logger.error(format(CONFIGURATION_BY_ID_ERROR_MESSAGE_TEMPLATE, "deleting", configurationId, e.getMessage()), e);
        asyncResultHandler.handle(Future.succeededFuture(
            DeleteOaiPmhConfigurationsByConfigurationIdResponse.respond500WithApplicationJson(
                createErrorResponse(e.getMessage()))));
      }
    });
  }

  @Override
  public void getOaiPmhConfigurationsByNameByConfigName(String configName,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    
    vertxContext.runOnContext(v -> {
      try {
        String tenantId = TenantTool.tenantId(okapiHeaders);
        logger.info("Getting configuration by name: {} for tenant: {}", configName, tenantId);

        configurationCrudService.getConfigurationByName(configName, tenantId)
          .onSuccess(configuration -> {
            if (configuration != null) {
              asyncResultHandler.handle(Future.succeededFuture(
                  GetOaiPmhConfigurationsByNameByConfigNameResponse.respond200WithApplicationJson(configuration)));
            } else {
              asyncResultHandler.handle(Future.succeededFuture(
                  GetOaiPmhConfigurationsByNameByConfigNameResponse.respond404WithTextPlain(CONFIGURATION_NOT_FOUND_ERROR)));
            }
          })
          .onFailure(throwable -> {
            logger.error(format(CONFIGURATION_ERROR_MESSAGE_TEMPLATE, "getting by name", throwable.getMessage()), throwable);
            asyncResultHandler.handle(Future.succeededFuture(
                GetOaiPmhConfigurationsByNameByConfigNameResponse.respond500WithApplicationJson(
                    createErrorResponse(throwable.getMessage()))));
          });
      } catch (Exception e) {
        logger.error(format(CONFIGURATION_ERROR_MESSAGE_TEMPLATE, "getting by name", e.getMessage()), e);
        asyncResultHandler.handle(Future.succeededFuture(
            GetOaiPmhConfigurationsByNameByConfigNameResponse.respond500WithApplicationJson(
                createErrorResponse(e.getMessage()))));
      }
    });
  }

  private Errors createErrorResponse(String message) {
    return new Errors()
        .withErrors(List.of(new Error()
            .withMessage(message)
            .withCode("-1")
            .withType("1")));
  }
}
