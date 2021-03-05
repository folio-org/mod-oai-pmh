package org.folio.oaipmh.client;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

import org.folio.rest.tools.ClientHelpers;
import org.folio.rest.tools.utils.VertxUtils;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

//TODO Should be replaced with the SourceStorageSourceRecordsClient from mod-source-record-storage-client
// when it's upgraded to RMD version 32 and Vert.x version 4
public class SourceStorageSourceRecordsClient {
  private static final String GLOBAL_PATH = "/source-storage/source-records";
  protected String tenantId;
  protected String token;
  protected String okapiUrl;
  private HttpClientOptions options;
  private HttpClient httpClient;

  private static final Logger logger = LoggerFactory.getLogger(SourceStorageSourceRecordsClient.class);

  public SourceStorageSourceRecordsClient(String okapiUrl, String tenantId, String token, boolean keepAlive, int connTO, int idleTO) {
    this.tenantId = tenantId;
    this.token = token;
    this.okapiUrl = okapiUrl;
    this.options = new HttpClientOptions();
    this.options.setLogActivity(true);
    this.options.setKeepAlive(keepAlive);
    this.options.setConnectTimeout(connTO);
    this.options.setIdleTimeout(idleTO);
    this.httpClient = VertxUtils.getVertxFromContextOrNew().createHttpClient(this.options);
  }

  public SourceStorageSourceRecordsClient(String okapiUrl, String tenantId, String token) {
    this(okapiUrl, tenantId, token, true, 2000, 20);
  }

  public SourceStorageSourceRecordsClient(SourceStorageSourceRecordsClient client) {
    this(client.okapiUrl, client.tenantId, client.token, true, 2000, 20);
  }

  public void postSourceStorageSourceRecords(String idType, Boolean deleted, List List, Handler<HttpClientResponse> responseHandler, Handler<Throwable> exceptionHandler ) throws UnsupportedEncodingException, Exception {
    StringBuilder queryParams = new StringBuilder("?");
    if (idType != null) {
      queryParams.append("idType=");
      queryParams.append(URLEncoder.encode(idType, "UTF-8"));
      queryParams.append("&");
    }

    if (deleted != null) {
      queryParams.append("deleted=");
      queryParams.append(deleted);
      queryParams.append("&");
    }

    Buffer buffer = Buffer.buffer();
    if (List != null) {
      buffer.appendString(ClientHelpers.pojo2json(List));
    }


    HttpClientRequest request = this.httpClient.postAbs(this.okapiUrl + "/source-storage/source-records" + queryParams.toString());
    request.handler(responseHandler);
    request.exceptionHandler(e-> {
      logger.error("SRS response error 1234: " + e.getMessage(), e);
      exceptionHandler.handle(e);
    });
    request.setTimeout(10000);
    request.putHeader("Content-type", "application/json");
    request.putHeader("Accept", "application/json,text/plain");
    if (this.tenantId != null) {
      request.putHeader("X-Okapi-Token", this.token);
      request.putHeader("x-okapi-tenant", this.tenantId);
    }

    if (this.okapiUrl != null) {
      request.putHeader("X-Okapi-Url", this.okapiUrl);
    }

    request.putHeader("Content-Length", buffer.length() + "");
    request.setChunked(true);
    request.write(buffer);
    request.end();
  }

  public void close() {
    httpClient.close();
  }
}
