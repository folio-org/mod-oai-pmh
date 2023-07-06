package org.folio.oaipmh.service;

import static org.folio.oaipmh.service.MetricsCollectingService.MetricOperation.SRS_RESPONSE;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.folio.oaipmh.Request;
import org.folio.oaipmh.WebClientProvider;
import org.folio.rest.client.SourceStorageSourceRecordsClient;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;

public class SourceStorageSourceRecordsClientWrapper {

  private final MetricsCollectingService metricsCollectingService = MetricsCollectingService.getInstance();
  private static final Map<String, SourceStorageSourceRecordsClientWrapper> clients = new HashMap<>();

  private final SourceStorageSourceRecordsClient client;

  private SourceStorageSourceRecordsClientWrapper(Request request) {
    client = new SourceStorageSourceRecordsClient(request.getOkapiUrl(), request.getTenant(), request.getOkapiToken(),
      WebClientProvider.getWebClientForSRSByTenant(request.getTenant(), request.getRequestId()));
  }

  public static SourceStorageSourceRecordsClientWrapper getSourceStorageSourceRecordsClient(Request request)
  {
    return clients.computeIfAbsent(request.getTenant(), tenantId -> new SourceStorageSourceRecordsClientWrapper(request));
  }

  public void postSourceStorageSourceRecords(String idType, String recordType, Boolean deleted, List<String> list,
      Handler<AsyncResult<HttpResponse<Buffer>>> responseHandler) {
    var requestId = UUID.randomUUID().toString();
    metricsCollectingService.startMetric(requestId, SRS_RESPONSE);
    client.postSourceStorageSourceRecords(idType, recordType, deleted, list)
      .onComplete(httpResponseAsyncResult -> {
        metricsCollectingService.endMetric(requestId, SRS_RESPONSE);
        responseHandler.handle(httpResponseAsyncResult);
      });
  }

  public void getSourceStorageSourceRecords(String recordId, String snapshotId, String externalId, String externalHrid,
      String instanceId, String instanceHrid, String holdingsId, String holdingsHrid, String recordType,
      Boolean suppressFromDiscovery, Boolean deleted, String leaderRecordStatus, Date updatedAfter, Date updatedBefore,
      String[] orderBy, int offset, int limit, Handler<AsyncResult<HttpResponse<Buffer>>> responseHandler) {
    var requestId = UUID.randomUUID().toString();
    metricsCollectingService.startMetric(requestId, SRS_RESPONSE);
    client
      .getSourceStorageSourceRecords(recordId, snapshotId, externalId, externalHrid, instanceId, instanceHrid, holdingsId,
          holdingsHrid, recordType, suppressFromDiscovery, deleted, leaderRecordStatus, updatedAfter, updatedBefore, orderBy,
          offset, limit)
      .onComplete(httpResponseAsyncResult -> {
        metricsCollectingService.endMetric(requestId, SRS_RESPONSE);
        responseHandler.handle(httpResponseAsyncResult);
      });
  }
}
