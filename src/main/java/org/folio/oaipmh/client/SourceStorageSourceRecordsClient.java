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

  private static final Logger logger = LoggerFactory.getLogger(SourceStorageSourceRecordsClient.class);

  private static final String GLOBAL_PATH = "/source-storage/source-records";
  private static final int DEFAULT_SRS_TIMEOUT = 10000;
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 2000;
  private static final int DEFAULT_IDLE_TIMEOUT_SEC = 20;

  protected String tenantId;
  protected String token;
  protected String okapiUrl;
  private HttpClientOptions options;
  private HttpClient httpClient;

  public SourceStorageSourceRecordsClient(String okapiUrl, String tenantId, String token, boolean keepAlive, int connTO, int idleTimeoutSec) {
    this.tenantId = tenantId;
    this.token = token;
    this.okapiUrl = okapiUrl;
    this.options = new HttpClientOptions();
    this.options.setLogActivity(true);
    this.options.setKeepAlive(keepAlive);
    this.options.setConnectTimeout(connTO);
    this.options.setIdleTimeout(idleTimeoutSec);
    this.httpClient = VertxUtils.getVertxFromContextOrNew().createHttpClient(this.options);
  }

  public SourceStorageSourceRecordsClient(String okapiUrl, String tenantId, String token) {
    this(okapiUrl, tenantId, token, true, DEFAULT_CONNECTION_TIMEOUT_MS, DEFAULT_IDLE_TIMEOUT_SEC);
  }

  public SourceStorageSourceRecordsClient(SourceStorageSourceRecordsClient client) {
    this(client.okapiUrl, client.tenantId, client.token, true, DEFAULT_CONNECTION_TIMEOUT_MS, DEFAULT_IDLE_TIMEOUT_SEC);
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

    HttpClientRequest request = this.httpClient.postAbs(this.okapiUrl + GLOBAL_PATH + queryParams.toString());
    request.handler(responseHandler);
    request.exceptionHandler(e-> {
      logger.error("Error has been occurred while requesting SRS: " + e.getMessage(), e);
      exceptionHandler.handle(e);
    });
    request.setTimeout(DEFAULT_SRS_TIMEOUT);
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
