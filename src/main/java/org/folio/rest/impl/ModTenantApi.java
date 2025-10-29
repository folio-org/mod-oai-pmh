package org.folio.rest.impl;



import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.client.exceptions.ResponseException;
import org.folio.spring.SpringContextUtil;
import org.glassfish.jersey.message.internal.Statuses;


public class ModTenantApi extends TenantAPI {

  private final Logger logger = LogManager.getLogger(ModTenantApi.class);





  public ModTenantApi() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void postTenant(final TenantAttributes entity, final Map<String, String> headers,
      final Handler<AsyncResult<Response>> handlers, final Context context) {
    super.postTenant(entity, headers, postTenantAsyncResultHandler -> {
      if (postTenantAsyncResultHandler.failed()) {
        handlers.handle(postTenantAsyncResultHandler);
      } else {
        List<String> configsSet = Arrays.asList("behavior", "general", "technical");
        loadConfigurationData(headers, configsSet).onComplete(asyncResult -> {
          if (asyncResult.succeeded()) {
            handlers.handle(Future.succeededFuture(buildSuccessResponse(asyncResult.result())));
          } else {
            logger.error(asyncResult.cause());
            handlers.handle(Future.failedFuture(
                new ResponseException(buildErrorResponse(asyncResult.cause().getMessage()))));
          }
        });
      }
    }, context);
  }

  @Override
  Future<Integer> loadData(TenantAttributes attributes, String tenantId,
      Map<String, String> headers, Context vertxContext) {
    return super.loadData(attributes, tenantId, headers, vertxContext).compose(num -> {
      Vertx vertx = vertxContext.owner();
      LiquibaseUtil.initializeSchemaForTenant(vertx, tenantId);
      return Future.succeededFuture(num);
    });
  }

  public Future<String> loadConfigurationData(Map<String, String> headers,
      List<String> configsSet) {
    // Configuration is now stored in the database and loaded via ConfigurationService
    // The database migration handles creating the configuration_settings table with default values
    logger.info("Configuration data loading completed. Configuration is now stored in database.");
    return Future.succeededFuture("Configuration has been set up successfully from database.");
  }





  private Response buildSuccessResponse(String body) {
    Response.ResponseBuilder builder = Response.status(HttpStatus.SC_OK)
        .header(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.TEXT_PLAIN.toString())
        .entity(body);
    return builder.build();
  }

  private Response buildErrorResponse(String info) {
    Response.ResponseBuilder builder = Response.status(Statuses.from(400, info));
    return builder.build();
  }



}
