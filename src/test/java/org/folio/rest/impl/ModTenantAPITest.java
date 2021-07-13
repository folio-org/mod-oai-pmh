package org.folio.rest.impl;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.Map;
import java.util.TreeMap;
import org.folio.config.ApplicationConfig;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.spring.SpringContextUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class ModTenantAPITest {

  int okapiPort = -1;
  Context context;
  ModTenantAPI modTenantAPI;
  TenantAttributes tenantAttributes = new TenantAttributes().withModuleTo("99.99.99");

  void withModTenantAPI(Vertx vertx, VertxTestContext vtc, Runnable run) {
    vertx.exceptionHandler(e -> {
      e.printStackTrace();
      vtc.failNow(e);
    });
    vertx.runOnContext(x -> {
      context = vertx.getOrCreateContext();
      SpringContextUtil.init(vertx, context, ApplicationConfig.class);
      modTenantAPI = new ModTenantAPI();
      SpringContextUtil.autowireDependencies(modTenantAPI, context);
      okapiMock(vertx)
      .onComplete(vtc.succeeding(y -> run.run()));
    });
  }

  Future<HttpServer> okapiMock(Vertx vertx) {
    return vertx.createHttpServer()
        .requestHandler(httpServerRequest -> {
          switch (httpServerRequest.method().toString()) {
          case "POST":
            httpServerRequest.response().setStatusCode(201).end();
            break;
          default:
            httpServerRequest.response().end("{\"configs\":[]}");
            break;
          }
        })
        .listen(0)
        .onSuccess(httpServer -> okapiPort = httpServer.actualPort());
  }

  Map<String,String> headers() {
    var headers = new TreeMap<String,String>(String.CASE_INSENSITIVE_ORDER);
    headers.put("x-okapi-url", "http://localhost:" + okapiPort);
    headers.put("x-okapi-tenant", "diku");
    return headers;
  }

  @Test
  void postTenantShouldSucceed(Vertx vertx, VertxTestContext vtc) {
    withModTenantAPI(vertx, vtc, () -> {
      modTenantAPI.postTenant(tenantAttributes, headers(), vtc.succeedingThenComplete(), context);
    });
  }

  @Test
  void postTenantShouldFail_whenNoTenantId(Vertx vertx, VertxTestContext vtc) {
    var headers = headers();
    headers.remove("x-okapi-tenant");
    withModTenantAPI(vertx, vtc, () -> {
      modTenantAPI.postTenant(tenantAttributes, headers, vtc.failingThenComplete(), context);
    });
  }

}
