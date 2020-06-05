package org.folio.oaipmh.processors;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.oaipmh.Constants.GENERIC_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.LIST_ILLEGAL_ARGUMENTS_ERROR;
import static org.folio.oaipmh.Constants.NEXT_RECORD_ID_PARAM;
import static org.folio.oaipmh.Constants.OFFSET_PARAM;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.AbstractHelper;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.helpers.streaming.BatchStreamWrapper;
import org.folio.rest.client.SourceStorageClient;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.StatusType;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
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
        final Object writeStreamObj = vertxContext.get(resumptionToken);
        if (!(writeStreamObj instanceof BatchStreamWrapper)) { // resumption token doesn't exist in context
          handleException(promise, new IllegalArgumentException(
            "Resumption token +" + resumptionToken + "+ doesn't exist in context"));
          return promise.future();
        }
        writeStream = (BatchStreamWrapper) writeStreamObj;
      }

      writeStream.handleBatch(batch -> {
        SourceStorageClient srsClient = new SourceStorageClient(request.getOkapiUrl(),
          request.getTenant(), request.getOkapiToken());
        Map<String, JsonObject> mapFuture = requestSRSByIdentifiers(vertxContext,
          srsClient, request, batch);
        boolean theLastBatch = batch.size() < batchSize || writeStream.isStreamEnded();

        //TODO MERGE INVENTORY AND SRS RESPONSES AND MAP TO OAI-PMH FIELDS

        promise.complete(buildRecordsResponse(request, requestId, batch, mapFuture,
          writeStream.getCount(), !theLastBatch));
        if (theLastBatch) {
          vertxContext.remove(requestId);
        }
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

  private Response buildRecordsResponse(
    Request request, String requestId, List<JsonEvent> batch,
    Map<String, JsonObject> mapFuture, long count,
    boolean returnResumptionToken) {

    List<RecordType> records = buildRecordsList(request, batch, mapFuture);

    ResponseHelper responseHelper = getResponseHelper();
    OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request);
    if(records.isEmpty()) {
      oaipmh.withErrors(createNoRecordsFoundError());
    } else {
      oaipmh.withListRecords(new ListRecordsType().withRecords(records));
    }

    if(returnResumptionToken) {
      ResumptionTokenType resumptionToken = buildResumptionTokenFromRequest(request, requestId, count);
      oaipmh.getListRecords().withResumptionToken(resumptionToken);
    }

    Response response;
    if(oaipmh.getErrors().isEmpty()) {
      response = responseHelper.buildSuccessResponse(oaipmh);
    } else {
      response = responseHelper.buildFailureResponse(oaipmh, request);
    }
    return response;
  }

  private List<RecordType> buildRecordsList(Request request, List<JsonEvent> batch, Map<String, JsonObject> mapFuture) {
    RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
    final boolean isDeletedRecordsEnabled = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request);

    List<RecordType> records = new ArrayList<>();

    batch.forEach(jsonEvent -> {
      final JsonObject inventoryInstance = (JsonObject) jsonEvent.value();
      final String instanceId = inventoryInstance.getString("instanceid");
      final JsonObject srsInstance = mapFuture.get(instanceId);

      JsonObject updatedSrsInstance = metadataManager.populateMetadataWithItemsData(srsInstance, inventoryInstance);
      String identifierPrefix = request.getIdentifierPrefix();
      RecordType record = new RecordType()
        .withHeader(createHeader(inventoryInstance)
          .withIdentifier(getIdentifier(instanceId, identifierPrefix)));

      if (isDeletedRecordsEnabled && storageHelper.isRecordMarkAsDeleted(updatedSrsInstance)) {
        record.getHeader().setStatus(StatusType.DELETED);
      }
      String source = storageHelper.getInstanceRecordSource(updatedSrsInstance);

      if (source != null && record.getHeader().getStatus() != StatusType.DELETED) {
        source = metadataManager.updateMetadataSourceWithDiscoverySuppressedDataIfNecessary(source, updatedSrsInstance, request);
        record.withMetadata(buildOaiMetadata(request, source));
      }
      records.add(record);
    });
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


    private BatchStreamWrapper createBatchStream (Request request,
      Promise < Response > oaiPmhResponsePromise,
      Context vertxContext,int batchSize, String resumptionToken){
      final Vertx vertx = vertxContext.owner();
      final HttpClient inventoryHttpClient = vertx.createHttpClient();

      BatchStreamWrapper writeStream = new BatchStreamWrapper(vertx, batchSize);
      vertxContext.put(resumptionToken, writeStream);

      String inventoryQuery = buildInventoryQuery(request);
      logger.info("Sending request to {0}", inventoryQuery);

      final HttpClientRequest httpClientRequest = inventoryHttpClient
        .getAbs(inventoryQuery);
      httpClientRequest.handler(resp -> {
        JsonParser jp = new JsonParserImpl(resp);
        jp.objectValueMode();
        jp.pipeTo(writeStream, ar -> {
          //TODO - end of stream here
        });
        jp.endHandler(e -> {
          inventoryHttpClient.close();
        });
      });
      httpClientRequest.exceptionHandler(e -> handleException(oaiPmhResponsePromise, e));
      httpClientRequest.end();
      return writeStream;
    }


    private CompletableFuture<Response> buildNoRecordsFoundOaiResponse (OAIPMH
    oaipmh,
      Request request){
      oaipmh.withErrors(createNoRecordsFoundError());
      return completedFuture(getResponseHelper().buildFailureResponse(oaipmh, request));
    }

    //TODO RETURN TYPE
    private Map<String, JsonObject> requestSRSByIdentifiers (Context ctx,
      SourceStorageClient srsClient, Request request,
      List < JsonEvent > batch){

      //todo go to srs
      //todo build query 'or'

      final String srsRequest = buildSrsRequest(batch);
      logger.info("Request to SRS: {0}", srsRequest);

      //TODO EMPTY RESPONSE?
      //TODO ERROR?
      try {
        srsClient.getSourceStorageRecords(srsRequest, 0, batch.size(), null, rh -> {

          rh.bodyHandler(bh -> {
            final Object o = bh.toJson();

            System.out.println("bh = " + bh);
          });

        });
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }

      return new HashMap<>();
    }

    //todo join instanceIds with OR
    private String buildSrsRequest (List < JsonEvent > batch) {

      final StringBuilder request = new StringBuilder("(");

      for (JsonEvent jsonEvent : batch) {
        final Object aggregatedInstanceObject = jsonEvent.value();
        if (aggregatedInstanceObject instanceof JsonObject) {
          JsonObject instance = (JsonObject) aggregatedInstanceObject;
          final String identifier = instance.getString("identifier");
          request.append("externalIdsHolder.identifier=");
          request.append(identifier);
        }
      }
      request.append(")");
      return request.toString();
    }


    private void handleException (Promise < Response > promise, Throwable e){
      logger.error(GENERIC_ERROR_MESSAGE, e);
      promise.fail(e);
    }


  }
