package org.folio.oaipmh.processors;

import static io.vertx.core.http.HttpMethod.GET;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.oaipmh.Constants.GENERIC_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.LIST_ILLEGAL_ARGUMENTS_ERROR;
import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

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
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.commons.lang.StringUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.AbstractHelper;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.helpers.streaming.BatchStreamWrapper;
import org.folio.rest.client.SourceStorageClient;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;

public class MarcWithHoldingsRequestHelper extends AbstractHelper {

  private static final int BATCH_SIZE = 1000;
  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public static final MarcWithHoldingsRequestHelper INSTANCE = new MarcWithHoldingsRequestHelper();


  public static MarcWithHoldingsRequestHelper getInstance() {
    return INSTANCE;
  }


  protected ResponseHelper getResponseHelper() {
    return ResponseHelper.getInstance();
  }

  @Override
  public Future<Response> handle(Request request, Context vertxContext) {
    Promise<Response> oaiPmhResponsePromise = Promise.promise();
    try {
      if (request.getResumptionToken() != null && !request.restoreFromResumptionToken()) {
        ResponseHelper responseHelper = getResponseHelper();
        OAIPMH oaipmh = responseHelper
          .buildOaipmhResponseWithErrors(request, BAD_ARGUMENT, LIST_ILLEGAL_ARGUMENTS_ERROR);
        oaiPmhResponsePromise.complete(responseHelper.buildFailureResponse(oaipmh, request));
        return oaiPmhResponsePromise.future();
      }

      List<OAIPMHerrorType> errors = validateListRequest(request);
      if (!errors.isEmpty()) {
        ResponseHelper responseHelper = getResponseHelper();
        OAIPMH oai;
        if (request.isRestored()) {
          oai = responseHelper.buildOaipmhResponseWithErrors(request, BAD_RESUMPTION_TOKEN,
            RESUMPTION_TOKEN_FORMAT_ERROR);
        } else {
          oai = responseHelper.buildOaipmhResponseWithErrors(request, errors);
        }
        oaiPmhResponsePromise.complete(responseHelper.buildFailureResponse(oai, request));
        return oaiPmhResponsePromise.future();
      }

      //TODO MOVE HTTP CLIENTS TO OTHER CLASSES
      BatchStreamWrapper writeStream;
      if (request.getResumptionToken() == null) { // the first request from EDS
        writeStream = createBatchStream(request, oaiPmhResponsePromise, vertxContext);
      } else {
        final Object writeStreamObj = vertxContext.get(request.getResumptionToken());
        if (!(writeStreamObj instanceof BatchStreamWrapper)) { // resumption token doesn't exist in context
          handleException(oaiPmhResponsePromise.future(), new IllegalArgumentException(
            "Resumption token +" + request.getResumptionToken() + "+ doesn't exist in context"));
          return oaiPmhResponsePromise.future();
        }
        writeStream = (BatchStreamWrapper) writeStreamObj;
      }

      writeStream.handleBatch(batch -> {
        SourceStorageClient srsClient = new SourceStorageClient(request.getOkapiUrl(),
          request.getTenant(), request.getOkapiToken());
        Map<String, JsonObject> mapFuture = requestSRSByIdentifiers(vertxContext,
          srsClient, request, batch);
        boolean theLastBatch = batch.size() < BATCH_SIZE || writeStream.isStreamEnded();

        //TODO MERGE INVENTORY AND SRS RESPONSES AND MAP TO OAI-PMH FIELDS

        oaiPmhResponsePromise.complete(buildRecordsResponse(batch, mapFuture, !theLastBatch));
        if (theLastBatch) {
          vertxContext.remove(request.getResumptionToken());
        }
      });
    } catch (Exception e) {
      handleException(oaiPmhResponsePromise.future(), e);
    }
    return oaiPmhResponsePromise.future();
  }

  private Response buildRecordsResponse(
    List<JsonEvent> batch, Map<String, JsonObject> mapFuture, boolean returnResumptionToken) {
    return null;
  }

  private String buildInventoryQuery(String inventoryEndpoint, Request request) {

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
        RepositoryConfigurationUtil.isDeletedRecordsEnabled(request, REPOSITORY_DELETED_RECORDS)));
    paramMap.put("skipSuppressedFromDiscoveryRecords",
      String.valueOf(
        getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING)));

    final String params = paramMap.entrySet().stream()
      .map(e -> e.getKey() + "=" + e.getValue())
      .collect(Collectors.joining("&"));

    return String.format("%s?%s", inventoryEndpoint, params);
  }


  private BatchStreamWrapper createBatchStream(Request request,
    Promise<Response> oaiPmhResponsePromise, Context vertxContext) {
    final Vertx vertx = vertxContext.owner();
    final HttpClient inventoryHttpClient = vertx.createHttpClient();
    //todo add params from request
    BatchStreamWrapper writeStream = new BatchStreamWrapper(vertx, BATCH_SIZE);
    //TODO: possibly set batch size from configuration
    vertxContext.put(request.getResumptionToken(), writeStream);
    String inventoryEndpoint = "/oai-pmh-view/instances";
    final String inventoryQuery = buildInventoryQuery(inventoryEndpoint, request);
    logger.debug("Sending message to {}", inventoryQuery);
    final HttpClientRequest httpClientRequest = inventoryHttpClient
      .requestAbs(GET, inventoryQuery);
    httpClientRequest.handler(resp -> {
      JsonParser jp = new JsonParserImpl(resp);
      jp.objectValueMode();
      jp.pipeTo(writeStream);
      jp.endHandler(e -> {
        inventoryHttpClient.close();
      });
    });
    httpClientRequest.exceptionHandler(e -> handleException(oaiPmhResponsePromise.future(), e));
    return writeStream;
  }


  private CompletableFuture<Response> buildNoRecordsFoundOaiResponse(OAIPMH oaipmh,
    Request request) {
    oaipmh.withErrors(createNoRecordsFoundError());
    return completedFuture(getResponseHelper().buildFailureResponse(oaipmh, request));
  }

  //TODO RETURN TYPE
  private Map<String, JsonObject> requestSRSByIdentifiers(Context ctx,
    SourceStorageClient srsClient, Request request,
    List<JsonEvent> batch) {

    //todo go to srs
    //todo build query 'or'

    final String srsRequest = buildSrsRequest(batch);

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
  private String buildSrsRequest(List<JsonEvent> batch) {

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


  private void handleException(Future<Response> future, Throwable e) {
    logger.error(GENERIC_ERROR_MESSAGE, e);
    future.fail(e);
  }


}
