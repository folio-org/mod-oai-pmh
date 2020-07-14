package org.folio.oaipmh.processors;

import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.NEXT_RECORD_ID_PARAM;
import static org.folio.oaipmh.Constants.OFFSET_PARAM;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;

import java.math.BigInteger;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import org.apache.commons.collections4.CollectionUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.AbstractHelper;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.helpers.streaming.BatchStreamWrapper;
import org.folio.rest.client.SourceStorageSourceRecordsClient;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.StatusType;

import com.google.common.collect.Maps;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.parsetools.impl.JsonParserImpl;

public class MarcWithHoldingsRequestHelper extends AbstractHelper {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public static final MarcWithHoldingsRequestHelper INSTANCE = new MarcWithHoldingsRequestHelper();

  private final Map<String, BatchStreamWrapper> activeStreams = new ConcurrentHashMap<>();

  /**
   * The dates returned by inventory storage service are in format "2018-09-19T02:52:08.873+0000".
   * Using {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME} and just in case 2 offsets "+HHmm" and "+HH:MM"
   */
  private static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    .optionalStart().appendOffset("+HH:MM", "Z").optionalEnd()
    .optionalStart().appendOffset("+HHmm", "Z").optionalEnd()
    .toFormatter();

  public static MarcWithHoldingsRequestHelper getInstance() {
    return INSTANCE;
  }

  @Override
  public Future<Response> handle(Request request, Context vertxContext) {
    Promise<Response> promise = Promise.promise();
    try {

      String resumptionToken = request.getResumptionToken();
      final boolean deletedRecordSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request);
      List<OAIPMHerrorType> errors = validateListRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, promise, errors);
      }

      BatchStreamWrapper writeStream;
      int batchSize = Integer.parseInt(
        RepositoryConfigurationUtil.getProperty(request.getTenant(),
          REPOSITORY_MAX_RECORDS_PER_RESPONSE));
      String requestId =
        (resumptionToken == null || request.getNextRecordId() == null) ? UUID
          .randomUUID().toString() : request.getNextRecordId();
      if (resumptionToken == null
        || request.getNextRecordId() == null) { // the first request from EDS
        writeStream = createBatchStream(request, promise, vertxContext, batchSize, requestId);
      } else {
        writeStream = activeStreams.get(requestId);
        if (writeStream == null) { // resumption token doesn't exist in context
          handleException(promise, new IllegalArgumentException(
            "Resumption token +" + resumptionToken + "+ doesn't exist in context"));
          return promise.future();
        }
      }

      final SourceStorageSourceRecordsClient srsClient = new SourceStorageSourceRecordsClient(request.getOkapiUrl(),
        request.getTenant(), request.getOkapiToken());
      writeStream.exceptionHandler(e -> {
        if (e != null) {
          handleException(promise, e);
        }
      });
      writeStream.handleBatch(batch -> {
        boolean theLastBatch = batch.size() < batchSize ||
          (writeStream.isStreamEnded()
            && writeStream.getItemsInQueueCount() <= batchSize);
        if (theLastBatch) {
          activeStreams.remove(requestId);
        }

        Future<Map<String, JsonObject>> srsResponse = Future.future();
        if (CollectionUtils.isNotEmpty(batch)) {
          srsResponse = requestSRSByIdentifiers(srsClient, batch, deletedRecordSupport);
        } else {
          srsResponse.complete();
        }
        srsResponse.onSuccess(res -> buildRecordsResponse(request, requestId, batch, res,
          writeStream, !theLastBatch, deletedRecordSupport).onSuccess(promise::complete));
        srsResponse.onFailure(t -> handleException(promise, t));
      });

    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  private ResumptionTokenType buildResumptionTokenFromRequest(Request request, String id,
                                                              long returnedCount, boolean returnResumptionToken) {
    long offset = returnedCount + request.getOffset();
    if (!returnResumptionToken) {
      return new ResumptionTokenType()
        .withValue("")
        .withCursor(
          BigInteger.valueOf(offset));
    }
    Map<String, String> extraParams = new HashMap<>();
    extraParams.put(OFFSET_PARAM, String.valueOf(returnedCount));
    extraParams.put(NEXT_RECORD_ID_PARAM, id);
    if (request.getUntil() == null) {
      extraParams.put(UNTIL_PARAM, getUntilDate(request, request.getFrom()));
    }

    String resumptionToken = request.toResumptionToken(extraParams);

    return new ResumptionTokenType()
      .withValue(resumptionToken)
      .withCursor(
        request.getOffset() == 0 ? BigInteger.ZERO : BigInteger.valueOf(request.getOffset()));
  }

  private Future<Response> buildRecordsResponse(
    Request request, String requestId, List<JsonObject> batch,
    Map<String, JsonObject> srsResponse, boolean firstBatch,
    boolean returnResumptionToken, boolean deletedRecordSupport) {

    Promise<Response> promise = Promise.promise();
    try {
      List<RecordType> records = buildRecordsList(request, batch, srsResponse, deletedRecordSupport);

      ResponseHelper responseHelper = getResponseHelper();
      OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request);
      if (records.isEmpty() && !returnResumptionToken && (firstBatch && batch.isEmpty())) {
        oaipmh.withErrors(createNoRecordsFoundError());
      } else {
        oaipmh.withListRecords(new ListRecordsType().withRecords(records));
      }
      Response response;
      if (oaipmh.getErrors().isEmpty()) {
        if (!firstBatch || returnResumptionToken) {
          ResumptionTokenType resumptionToken = buildResumptionTokenFromRequest(request, requestId,
            records.size(), returnResumptionToken);
          oaipmh.getListRecords().withResumptionToken(resumptionToken);
        }
        response = responseHelper.buildSuccessResponse(oaipmh);
      } else {
        response = responseHelper.buildFailureResponse(oaipmh, request);
      }

      promise.complete(response);
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  private List<RecordType> buildRecordsList(Request request, List<JsonObject> batch, Map<String, JsonObject> srsResponse,
                                            boolean deletedRecordSupport) {
    RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();

    final boolean suppressedRecordsProcessing = getBooleanProperty(request.getOkapiHeaders(),
      REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

    List<RecordType> records = new ArrayList<>();

    for (JsonObject inventoryInstance : batch) {
      final String instanceId = inventoryInstance.getString("instanceid");
      final JsonObject srsInstance = srsResponse.get(instanceId);
      if (srsInstance == null) {
        continue;
      }
      JsonObject updatedSrsInstance = metadataManager
        .populateMetadataWithItemsData(srsInstance, inventoryInstance,
          suppressedRecordsProcessing);
      String identifierPrefix = request.getIdentifierPrefix();
      RecordType record = new RecordType()
        .withHeader(createHeader(inventoryInstance)
          .withIdentifier(getIdentifier(identifierPrefix, instanceId)));

      if (deletedRecordSupport && storageHelper.isRecordMarkAsDeleted(updatedSrsInstance)) {
        record.getHeader().setStatus(StatusType.DELETED);
      }
      String source = storageHelper.getInstanceRecordSource(updatedSrsInstance);
      if (source != null && record.getHeader().getStatus() == null) {
        if (suppressedRecordsProcessing) {
          source = metadataManager.updateMetadataSourceWithDiscoverySuppressedData(source, updatedSrsInstance);
          source = metadataManager.updateElectronicAccessFieldWithDiscoverySuppressedData(source, updatedSrsInstance);
        }
        record.withMetadata(buildOaiMetadata(request, source));
      }

      if (filterInstance(request, srsInstance)) {
        records.add(record);
      }
    }
    return records;
  }

  @Override
  protected HeaderType createHeader(JsonObject instance) {
    String instanceUpdatedDate = instance.getString("instanceupdateddate");
    Instant datetime = formatter.parse(instanceUpdatedDate, Instant::from)
      .truncatedTo(ChronoUnit.SECONDS);

    return new HeaderType()
      .withDatestamp(datetime)
      .withSetSpecs("all");
  }

  private String buildInventoryQuery(Request request) {
    final String inventoryEndpoint = "/oai-pmh-view/instances";
    Map<String, String> paramMap = new HashMap<>();
    Date date = convertStringToDate(request.getFrom(), false);
    if (date != null) {
        paramMap.put("startDate", dateFormat.format(date));
      }
    date = convertStringToDate(request.getUntil(), true);
    if (date != null) {
        paramMap.put("endDate", dateFormat.format(date));
      }
    paramMap.put("deletedRecordSupport",
      String.valueOf(
        RepositoryConfigurationUtil.isDeletedRecordsEnabled(request)));
    paramMap.put("skipSuppressedFromDiscoveryRecords",
      String.valueOf(
        !getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING)));

    final String params = paramMap.entrySet().stream()
      .map(e -> e.getKey() + "=" + e.getValue())
      .collect(Collectors.joining("&"));


    return String.format("%s%s?%s", request.getOkapiUrl(), inventoryEndpoint, params);
  }

  private BatchStreamWrapper createBatchStream(Request request,
                                               Promise<Response> oaiPmhResponsePromise,
                                               Context vertxContext, int batchSize, String resumptionToken) {
    final Vertx vertx = vertxContext.owner();

    BatchStreamWrapper writeStream = new BatchStreamWrapper(vertx, batchSize);
    activeStreams.put(resumptionToken, writeStream);

    final HttpClient inventoryHttpClient = vertx.createHttpClient();
    final HttpClientRequest httpClientRequest = createInventoryClientRequest(inventoryHttpClient,
      request);

    httpClientRequest.handler(resp -> {
      JsonParser jp = new JsonParserImpl(resp);
      jp.objectValueMode();
      jp.pipeTo(writeStream);
      jp.endHandler(e -> {
        writeStream.end();
        inventoryHttpClient.close();
      });
    });

    httpClientRequest.exceptionHandler(e -> {
      logger.error(e.getMessage(), e);
      activeStreams.remove(resumptionToken);
      handleException(oaiPmhResponsePromise, e);
    });
    httpClientRequest.sendHead();
    return writeStream;
  }

  private HttpClientRequest createInventoryClientRequest(HttpClient httpClient, Request request) {
    String inventoryQuery = buildInventoryQuery(request);

    logger.info("Sending request to {0}", inventoryQuery);
    final HttpClientRequest httpClientRequest = httpClient
      .getAbs(inventoryQuery);

    httpClientRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpClientRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpClientRequest.putHeader(ACCEPT, APPLICATION_JSON);

    return httpClientRequest;
  }

  private Future<Map<String, JsonObject>> requestSRSByIdentifiers(SourceStorageSourceRecordsClient srsClient,
                                                                  List<JsonEvent> batch, boolean deletedRecordSupport) {
    final List<String> listOfIds = extractListOfIdsForSRSRequest(batch);
    logger.info("Request to SRS: {0}", listOfIds);
    Promise<Map<String, JsonObject>> promise = Promise.promise();
    try {
      final Map<String, JsonObject> result = Maps.newHashMap();
      srsClient.postSourceStorageSourceRecords("INSTANCE", deletedRecordSupport, listOfIds, rh -> rh.bodyHandler(bh -> {
          try {
            final Object o = bh.toJson();
            if (o instanceof JsonObject) {
              JsonObject entries = (JsonObject) o;
              final JsonArray records = entries.getJsonArray("sourceRecords");
              records.stream()
                .filter(Objects::nonNull)
                .map(r -> (JsonObject) r)
                .forEach(jo -> result.put(jo.getJsonObject("externalIdsHolder").getString("instanceId"), jo));
            } else {
              logger.debug("Can't process response from SRS: {0}", bh.toString());
            }
            promise.complete(result);
          } catch (Exception e) {
            handleException(promise, e);
          }
        }
      ));
    } catch (Exception e) {
      handleException(promise, e);
    }

    return promise.future();
  }

  private List<String> extractListOfIdsForSRSRequest(List<JsonEvent> batch) {

    return batch.stream().map(JsonEvent::value).
      filter(aggregatedInstanceObject -> aggregatedInstanceObject instanceof JsonObject)
      .map(aggregatedInstanceObject -> (JsonObject) aggregatedInstanceObject)
      .map(instance -> instance.getString("instanceid"))
      .collect(Collectors.toList());
  }


  private void handleException(Promise<?> promise, Throwable e) {
    logger.error(e.getMessage(), e);
    promise.fail(e);
  }
}
