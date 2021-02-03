package org.folio.oaipmh.client;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

import org.folio.rest.tools.ClientHelpers;
import org.folio.rest.tools.utils.VertxUtils;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

//TODO Should be replaced with the SourceStorageSourceRecordsClient from mod-source-record-storage-client
// when it's upgraded to RMD version 32 and Vert.x version 4
public class SourceStorageSourceRecordsClient {
  private static final String GLOBAL_PATH = "/source-storage/source-records";
  private String tenantId;
  private String token;
  private String okapiUrl;
  private WebClientOptions options;
  private WebClient webClient;

  public SourceStorageSourceRecordsClient(String okapiUrl, String tenantId, String token, boolean keepAlive, int connTO, int idleTO) {
    this.tenantId = tenantId;
    this.token = token;
    this.okapiUrl = okapiUrl;
    this.options = new WebClientOptions();
    this.options.setLogActivity(true);
    this.options.setKeepAlive(keepAlive);
    this.options.setConnectTimeout(connTO);
    this.options.setIdleTimeout(idleTO);
    this.webClient = WebClient.create(VertxUtils.getVertxFromContextOrNew(), this.options);
  }

  public SourceStorageSourceRecordsClient(String okapiUrl, String tenantId, String token, boolean keepAlive) {
    this(okapiUrl, tenantId, token, keepAlive, 2000, 5000);
  }


  public SourceStorageSourceRecordsClient(String okapiUrl, String tenantId, String token) {
    this(okapiUrl, tenantId, token, true, 2000, 5000);
  }

  public Future<HttpResponse<Buffer>> getSourceStorageSourceRecords(String recordId, String snapshotId, String instanceId, String recordType, Boolean suppressFromDiscovery, Boolean deleted, String leaderRecordStatus, Date updatedAfter, Date updatedBefore, String[] orderBy, int offset, int limit) throws UnsupportedEncodingException {
    StringBuilder queryParams = new StringBuilder("?");
    if (recordId != null) {
      queryParams.append("recordId=");
      queryParams.append(URLEncoder.encode(recordId, "UTF-8"));
      queryParams.append("&");
    }

    if (snapshotId != null) {
      queryParams.append("snapshotId=");
      queryParams.append(URLEncoder.encode(snapshotId, "UTF-8"));
      queryParams.append("&");
    }

    if (instanceId != null) {
      queryParams.append("instanceId=");
      queryParams.append(URLEncoder.encode(instanceId, "UTF-8"));
      queryParams.append("&");
    }

    if (recordType != null) {
      queryParams.append("recordType=");
      queryParams.append(URLEncoder.encode(recordType, "UTF-8"));
      queryParams.append("&");
    }

    if (suppressFromDiscovery != null) {
      queryParams.append("suppressFromDiscovery=");
      queryParams.append(suppressFromDiscovery);
      queryParams.append("&");
    }

    if (deleted != null) {
      queryParams.append("deleted=");
      queryParams.append(deleted);
      queryParams.append("&");
    }

    if (leaderRecordStatus != null) {
      queryParams.append("leaderRecordStatus=");
      queryParams.append(URLEncoder.encode(leaderRecordStatus, "UTF-8"));
      queryParams.append("&");
    }

    if (updatedAfter != null) {
      queryParams.append("updatedAfter=");
      queryParams.append(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").format(ZonedDateTime.ofInstant(updatedAfter.toInstant(), ZoneId.of("UTC"))));
      queryParams.append("&");
    }

    if (updatedBefore != null) {
      queryParams.append("updatedBefore=");
      queryParams.append(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").format(ZonedDateTime.ofInstant(updatedBefore.toInstant(), ZoneId.of("UTC"))));
      queryParams.append("&");
    }

    if (orderBy != null) {
      queryParams.append("orderBy=");
      if (orderBy.getClass().isArray()) {
        queryParams.append(String.join("&orderBy=", orderBy));
      }

      queryParams.append("&");
    }

    queryParams.append("offset=");
    queryParams.append(offset);
    queryParams.append("&");
    queryParams.append("limit=");
    queryParams.append(limit);
    queryParams.append("&");
    HttpRequest<Buffer> request = this.webClient.getAbs(this.okapiUrl + "/source-storage/source-records" + queryParams.toString());
    request.putHeader("Accept", "application/json,text/plain");
    if (this.tenantId != null) {
      request.putHeader("X-Okapi-Token", this.token);
      request.putHeader("x-okapi-tenant", this.tenantId);
    }

    if (this.okapiUrl != null) {
      request.putHeader("X-Okapi-Url", this.okapiUrl);
    }

    return request.send();
  }

  public Future<HttpResponse<Buffer>> postSourceStorageSourceRecords(String idType, Boolean deleted, List list) throws Exception {
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
    if (list != null) {
      buffer.appendString(ClientHelpers.pojo2json(list));
    }

    HttpRequest<Buffer> request = this.webClient.postAbs(this.okapiUrl + "/source-storage/source-records" + queryParams.toString());
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
    return request.sendBuffer(buffer);
  }
}
