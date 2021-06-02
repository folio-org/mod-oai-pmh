package org.folio.oaipmh.client;

import static org.folio.oaipmh.Constants.REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.folio.rest.tools.ClientHelpers;
import org.folio.rest.tools.utils.VertxUtils;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

//TODO Should be replaced with the SourceStorageSourceRecordsClient from mod-source-record-storage-client
// when it's upgraded to RMD version 32 and Vert.x version 4
public class SourceStorageSourceRecordsClient {

  private static final Logger logger = LogManager.getLogger(SourceStorageSourceRecordsClient.class);

  private static final String SOURCE_RECORDS_PATH = "/source-storage/source-records";
  private static final int DEFAULT_SRS_TIMEOUT = 10000;
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 2000;
  private static final int DEFAULT_IDLE_TIMEOUT_SEC = 20;
  private static final String GET_IDLE_TIMEOUT_ERROR_MESSAGE = "Error occurred during resolving the idle timeout setting value. Setup client with default idle timeout " + DEFAULT_IDLE_TIMEOUT_SEC + " seconds";

  protected String tenantId;
  protected String token;
  protected String okapiUrl;
  private HttpClientOptions options;
  private HttpClient httpClient;

  public SourceStorageSourceRecordsClient(String okapiUrl, String tenantId, String token, boolean keepAlive, int connTO) {
    this.tenantId = tenantId;
    this.token = token;
    this.okapiUrl = okapiUrl;
    this.options = new HttpClientOptions();
    this.options.setLogActivity(true);
    this.options.setKeepAlive(keepAlive);
    this.options.setConnectTimeout(connTO);
    this.options.setIdleTimeout(getIdleTimeout(tenantId));
    this.httpClient = VertxUtils.getVertxFromContextOrNew().createHttpClient(this.options);
  }

  public SourceStorageSourceRecordsClient(String okapiUrl, String tenantId, String token) {
    this(okapiUrl, tenantId, token, true, DEFAULT_CONNECTION_TIMEOUT_MS);
  }

  public SourceStorageSourceRecordsClient(SourceStorageSourceRecordsClient client) {
    this(client.okapiUrl, client.tenantId, client.token, true, DEFAULT_CONNECTION_TIMEOUT_MS);
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

    HttpClientRequest request = this.httpClient.postAbs(this.okapiUrl + SOURCE_RECORDS_PATH + queryParams.toString());
    request.handler(responseHandler);
    request.exceptionHandler(exceptionHandler);
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

  private int getIdleTimeout(String tenantId) {
    String property = System.getProperty(REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC);
    final String defaultValue = Objects.nonNull(property) ? property : String.valueOf(DEFAULT_IDLE_TIMEOUT_SEC);
    String val = Optional.ofNullable(Vertx.currentContext().config().getJsonObject(tenantId))
      .map(config -> config.getString(REPOSITORY_SRS_CLIENT_IDLE_TIMEOUT_SEC, defaultValue))
      .orElse(defaultValue);
    try {
      int configValue = Integer.parseInt(val);
      logger.debug("Setup client with idle timeout '{}' seconds", configValue);
      return configValue;
    } catch (Exception e) {
      logger.error(GET_IDLE_TIMEOUT_ERROR_MESSAGE, e);
      return DEFAULT_IDLE_TIMEOUT_SEC;
    }
  }

}
