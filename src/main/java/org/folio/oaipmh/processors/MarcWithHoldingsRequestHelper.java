package org.folio.oaipmh.processors;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.GENERIC_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.LIST_ILLEGAL_ARGUMENTS_ERROR;
import static org.folio.oaipmh.Constants.NEXT_RECORD_ID_PARAM;
import static org.folio.oaipmh.Constants.OFFSET_PARAM;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.AbstractHelper;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.helpers.streaming.BatchStreamWrapper;
import org.folio.rest.client.SourceStorageClient;
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
      if (resumptionToken != null && !request.restoreFromResumptionToken()) {
        ResponseHelper responseHelper = getResponseHelper();
        OAIPMH oaipmh = getResponseHelper()
          .buildOaipmhResponseWithErrors(request, BAD_ARGUMENT, LIST_ILLEGAL_ARGUMENTS_ERROR);
        promise.complete(responseHelper.buildFailureResponse(oaipmh, request));
        return promise.future();
      }

      List<OAIPMHerrorType> errors = validateListRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, promise, errors);
      }

      BatchStreamWrapper writeStream;
      int batchSize = Integer.parseInt(
        RepositoryConfigurationUtil.getProperty(request.getOkapiHeaders().get(OKAPI_TENANT),
          REPOSITORY_MAX_RECORDS_PER_RESPONSE));
      String requestId =
        (resumptionToken == null || request.getNextRecordId() == null) ? UUID
          .randomUUID().toString() : request.getNextRecordId();
      if (resumptionToken == null
        || request.getNextRecordId() == null) { // the first request from EDS
        writeStream = createBatchStream(request, promise, vertxContext, batchSize, requestId);
      } else {
        final Object writeStreamObj = vertxContext.get(requestId);
        if (!(writeStreamObj instanceof BatchStreamWrapper)) { // resumption token doesn't exist in context
          handleException(promise, new IllegalArgumentException(
            "Resumption token +" + resumptionToken + "+ doesn't exist in context"));
          return promise.future();
        }
        writeStream = (BatchStreamWrapper) writeStreamObj;
      }

      final SourceStorageClient srsClient = new SourceStorageClient(request.getOkapiUrl(),
        request.getTenant(), request.getOkapiToken());
      writeStream.handleBatch(batch -> {
        boolean theLastBatch = batch.size() < batchSize || writeStream.isStreamEnded();
        if (theLastBatch) {
          vertxContext.remove(requestId);
        }

        Future<Map<String, JsonObject>> srsResponse = Future.future();
        if (CollectionUtils.isNotEmpty(batch)){
          srsResponse = requestSRSByIdentifiers(srsClient, batch);
        }else{
          srsResponse.complete();
        }
        srsResponse.onSuccess(res -> buildRecordsResponse(request, requestId, batch, res,
          writeStream.getCount(), !theLastBatch).onSuccess(promise::complete));
        srsResponse.onFailure(t-> handleException(promise, t));
      });

    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  private ResumptionTokenType buildResumptionTokenFromRequest(Request request, String id,
                                                              long offset) {
    Map<String, String> extraParams = new HashMap<>();
    extraParams.put(OFFSET_PARAM, String.valueOf(offset));
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
    Request request, String requestId, List<JsonEvent> batch,
    Map<String, JsonObject> srsResponse, long count,
    boolean returnResumptionToken) {

    Promise<Response> promise = Promise.promise();
    List<RecordType> records = buildRecordsList(request, batch, srsResponse);

    ResponseHelper responseHelper = getResponseHelper();
    OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request);
    if (records.isEmpty()) {
      buildNoRecordsFoundOaiResponse(oaipmh, request);
    } else {
      oaipmh.withListRecords(new ListRecordsType().withRecords(records));
    }

    if (returnResumptionToken) {
      ResumptionTokenType resumptionToken = buildResumptionTokenFromRequest(request, requestId, count);
      oaipmh.getListRecords().withResumptionToken(resumptionToken);
    }

    Response response;
    if (oaipmh.getErrors().isEmpty()) {
      response = responseHelper.buildSuccessResponse(oaipmh);
    } else {
      response = responseHelper.buildFailureResponse(oaipmh, request);
    }

    promise.complete(response);
    return promise.future();
  }

  private List<RecordType> buildRecordsList(Request request, List<JsonEvent> batch, Map<String, JsonObject> srsResponse) {
    RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
    final boolean isDeletedRecordsEnabled = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request);

    List<RecordType> records = new ArrayList<>();

    for (JsonEvent jsonEvent : batch) {
      final JsonObject inventoryInstance = (JsonObject) jsonEvent.value();
      final String instanceId = inventoryInstance.getString("instanceid");
      final JsonObject srsInstance = srsResponse.get(instanceId);
      if (srsInstance == null) {
        continue;
      }
      JsonObject updatedSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance);
      String identifierPrefix = request.getIdentifierPrefix();
      RecordType record = new RecordType()
        .withHeader(createHeader(inventoryInstance)
          .withIdentifier(getIdentifier(identifierPrefix, instanceId)));

      if (isDeletedRecordsEnabled && storageHelper.isRecordMarkAsDeleted(updatedSrsInstance)) {
        record.getHeader().setStatus(StatusType.DELETED);
      }
      String source = storageHelper.getInstanceRecordSource(updatedSrsInstance);

      if (source != null && record.getHeader().getStatus() == null) {
        source = metadataManager.updateMetadataSourceWithDiscoverySuppressedDataIfNecessary(source, updatedSrsInstance, request);
        record.withMetadata(buildOaiMetadata(request, source));
      }

      if(filterInstance(request, srsInstance)) {
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

    private String buildInventoryQuery (Request request){
      final String inventoryEndpoint = "/oai-pmh-view/instances";
      Map<String, String> paramMap = new HashMap<>();
      final String from = request.getFrom();
      if (StringUtils.isNotEmpty(from)) {
        paramMap.put("startDate", from);
      }
      final String until = request.getUntil();
      if (StringUtils.isNotEmpty(until)) {
        paramMap.put("endDate", until);
      }
      paramMap.put("deletedRecordSupport",
        String.valueOf(
          RepositoryConfigurationUtil.isDeletedRecordsEnabled(request)));
      paramMap.put("skipSuppressedFromDiscoveryRecords",
        String.valueOf(
          getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING)));

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
    vertxContext.put(resumptionToken, writeStream);

    final HttpClient inventoryHttpClient = vertx.createHttpClient();
    final HttpClientRequest httpClientRequest = createInventoryClientRequest(inventoryHttpClient, request);

    httpClientRequest.handler(resp -> {
      JsonParser jp = new JsonParserImpl(resp);
      jp.objectValueMode();
      jp.pipeTo(writeStream);
      jp.endHandler(e -> {
        writeStream.end();
        inventoryHttpClient.close();
      });
    });

    httpClientRequest.exceptionHandler(e -> handleException(oaiPmhResponsePromise, e));
    httpClientRequest.sendHead();
    return writeStream;
  }

  private HttpClientRequest createInventoryClientRequest(HttpClient httpClient, Request request) {
    String inventoryQuery =  buildInventoryQuery(request);

    logger.info("Sending request to {0}", inventoryQuery);
    final HttpClientRequest httpClientRequest = httpClient
      .getAbs(inventoryQuery);

    httpClientRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpClientRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpClientRequest.putHeader(ACCEPT, APPLICATION_JSON);

    return httpClientRequest;
  }

  private CompletableFuture<Response> buildNoRecordsFoundOaiResponse(OAIPMH
                                                                       oaipmh,
                                                                     Request request) {
    oaipmh.withErrors(createNoRecordsFoundError());
    return completedFuture(getResponseHelper().buildFailureResponse(oaipmh, request));
  }

  private Future<Map<String, JsonObject>> requestSRSByIdentifiers(SourceStorageClient srsClient,
                                                          List<JsonEvent> batch) {
    final String srsRequest = buildSrsRequest(batch);
    logger.info("Request to SRS: {0}", srsRequest);
    Promise<Map<String, JsonObject>> promise = Promise.promise();
    try {
      final Map<String, JsonObject> result = Maps.newHashMap();
      srsClient.getSourceStorageRecords(srsRequest, 0, batch.size(), null, rh -> rh.bodyHandler(bh -> {

        final Object o = bh.toJson();
          if (o instanceof JsonObject) {
            JsonObject entries = (JsonObject) o;
            final JsonArray records = entries.getJsonArray("records");
            records.stream()
              .map(r -> (JsonObject) r)
              .forEach(jo -> result.put(jo.getJsonObject("externalIdsHolder").getString("instanceId"), jo));
          } else {
            logger.debug("Can't process response from SRS: {0}", bh.toString());
          }
          promise.complete(result);
        }
      ));
    } catch (UnsupportedEncodingException e) {
      logger.debug("Can't process response from SRS. Error: {0}", e.getMessage());
      promise.fail(e);
    }

    return promise.future();
  }

  private String buildSrsRequest(List<JsonEvent> batch) {

    return batch.stream().map(JsonEvent::value).
      filter(aggregatedInstanceObject -> aggregatedInstanceObject instanceof JsonObject)
      .map(aggregatedInstanceObject -> (JsonObject) aggregatedInstanceObject)
      .map(instance -> instance.getString("instanceid"))
      .map(identifier -> "externalIdsHolder.instanceId==" + identifier)
      .collect(Collectors.joining(" or ", "(", ")"));
  }


  private void handleException(Promise<Response> promise, Throwable e) {
    logger.error(GENERIC_ERROR_MESSAGE, e);
    promise.fail(e);
  }
}
