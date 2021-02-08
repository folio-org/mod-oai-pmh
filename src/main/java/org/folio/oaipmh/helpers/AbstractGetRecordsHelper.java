package org.folio.oaipmh.helpers;

import static org.folio.oaipmh.Constants.GENERIC_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FLOW_ERROR;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.isDeletedRecordsEnabled;
import static org.folio.rest.tools.client.Response.isSuccess;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.client.SourceStorageSourceRecordsClient;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.StatusType;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;

public abstract class AbstractGetRecordsHelper extends AbstractHelper {

  protected final Logger logger = LogManager.getLogger(getClass());

  @Override
  public Future<Response> handle(Request request, Context ctx) {
    Promise<Response> promise = Promise.promise();
    try {
      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, promise, errors);
      }
      requestAndProcessSrsRecords(request, ctx, promise);
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  protected void requestAndProcessSrsRecords(Request request, Context ctx, Promise<Response> promise) throws UnsupportedEncodingException {
    final SourceStorageSourceRecordsClient srsClient = new SourceStorageSourceRecordsClient(request.getOkapiUrl(),
      request.getTenant(), request.getOkapiToken());

    final boolean deletedRecordsSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request);
    final boolean suppressedRecordsSupport = getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

    final Date updatedAfter = request.getFrom() == null ? null : convertStringToDate(request.getFrom(), false, true);
    final Date updatedBefore = request.getUntil() == null ? null : convertStringToDate(request.getUntil(), true, true);

    int batchSize = Integer.parseInt(
      RepositoryConfigurationUtil.getProperty(request.getTenant(),
        REPOSITORY_MAX_RECORDS_PER_RESPONSE));

    srsClient.getSourceStorageSourceRecords(
      null,
      null,
      request.getIdentifier() != null ? request.getStorageIdentifier() : null,
      "MARC",
      //1. NULL if we want suppressed and not suppressed, TRUE = ONLY SUPPRESSED FALSE = ONLY NOT SUPPRESSED
      //2. use suppressed from discovery filtering only when deleted record support is enabled
      deletedRecordsSupport ? null : suppressedRecordsSupport,
      deletedRecordsSupport,
      null,
      updatedAfter,
      updatedBefore,
      null,
      request.getOffset(),
      batchSize + 1)
      .onSuccess(response -> handleSrsRecordsResponse(response, request, ctx, promise))
      .onFailure(e -> {
        logger.error("Exception getting {}", request.getVerb().value(), e);
        promise.fail(e);
      });
  }

  private void handleSrsRecordsResponse(HttpResponse<Buffer> response, Request request, Context ctx, Promise<Response> promise) {
    try {
      if (isSuccess(response.statusCode())) {
        JsonObject srsRecords = response.bodyAsJsonObject();
        final Response responseCompletableFuture = processRecords(ctx, request, srsRecords);
        promise.complete(responseCompletableFuture);
      } else {
        logger.error("{} response from SRS status code: {}: {}", request.getVerb().value(), response.statusMessage(), response.statusCode());
        throw new IllegalStateException(response.statusMessage());
      }
    } catch (DecodeException ex) {
      String msg = "Invalid json has been returned from SRS, cannot parse response to json.";
      logger.error(msg, ex, ex.getMessage());
      promise.fail(new IllegalStateException(msg, ex));
    } catch (Exception e) {
      logger.error("Exception getting {}", request.getVerb()
        .value(), e);
      promise.fail(e);
    }
  }

  protected Response processRecords(Context ctx, Request request,
                                    JsonObject instancesResponseBody) {
    JsonArray instances = storageHelper.getItems(instancesResponseBody);
    Integer totalRecords = storageHelper.getTotalRecords(instancesResponseBody);

    logger.debug("{} entries retrieved out of {}", instances != null ? instances.size() : 0, totalRecords);

    if (request.isRestored() && !canResumeRequestSequence(request, totalRecords, instances)) {
      OAIPMH oaipmh = getResponseHelper().buildBaseOaipmhResponse(request).withErrors(new OAIPMHerrorType()
        .withCode(BAD_RESUMPTION_TOKEN)
        .withValue(RESUMPTION_TOKEN_FLOW_ERROR));
      return getResponseHelper().buildFailureResponse(oaipmh, request);
    }

    ResumptionTokenType resumptionToken = buildResumptionToken(request, instances, totalRecords);

    /*
     * According to OAI-PMH guidelines: it is recommended that the responseDate reflect the time of the repository's clock at the start
     * of any database query or search function necessary to answer the list request, rather than when the output is written.
     */
    final OAIPMH oaipmh = getResponseHelper().buildBaseOaipmhResponse(request);
    final Map<String, RecordType> recordsMap = buildRecords(ctx, request, instances);
    if (recordsMap.isEmpty()) {
      return buildNoRecordsFoundOaiResponse(oaipmh, request);
    } else {
      addRecordsToOaiResponse(oaipmh, recordsMap.values());
      addResumptionTokenToOaiResponse(oaipmh, resumptionToken);
      return buildResponse(oaipmh, request);
    }
  }

  /**
   * Builds {@link Map} with storage id as key and {@link RecordType} with populated header if there is any,
   * otherwise empty map is returned
   */
  private Map<String, RecordType> buildRecords(Context context, Request request, JsonArray instances) {
    final boolean suppressedRecordsProcessingEnabled = getBooleanProperty(request.getOkapiHeaders(),
      REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

    if (instances != null && !instances.isEmpty()) {
      Map<String, RecordType> records = new HashMap<>();
      RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
      // Using LinkedHashMap just to rely on order returned by storage service
      String identifierPrefix = request.getIdentifierPrefix();
      instances.stream()
        .map(JsonObject.class::cast)
        .filter(instance -> StringUtils.isNotEmpty(storageHelper.getIdentifierId(instance)))
        .forEach(instance -> {
          String recordId = storageHelper.getRecordId(instance);
          String identifierId = storageHelper.getIdentifierId(instance);
          RecordType record = createRecord(request, identifierPrefix, instance, identifierId);
          // Some repositories like SRS can return record source data along with other info
          String source = storageHelper.getInstanceRecordSource(instance);
          if (source != null && record.getHeader().getStatus() == null) {
            if (suppressedRecordsProcessingEnabled) {
              source = metadataManager.updateMetadataSourceWithDiscoverySuppressedData(source, instance);
            }
            try {
              record.withMetadata(buildOaiMetadata(request, source));
            } catch (Exception e) {
              logger.error("Error occurred while converting record to xml representation.", e, e.getMessage());
              logger.debug("Skipping problematic record due the conversion error. Source record id - " + recordId);
              return;
            }
          } else {
            context.put(recordId, instance);
          }
          if (filterInstance(request, instance)) {
            records.put(recordId, record);
          }
        });
      return records;
    }
    return Collections.emptyMap();
  }

  private RecordType createRecord(Request request, String identifierPrefix, JsonObject instance, String identifierId) {
    RecordType record = new RecordType()
      .withHeader(createHeader(instance, request)
        .withIdentifier(getIdentifier(identifierPrefix, identifierId)));
    if (isDeletedRecordsEnabled(request) && storageHelper.isRecordMarkAsDeleted(instance)) {
      record.getHeader().setStatus(StatusType.DELETED);
    }
    return record;
  }

  private Response buildNoRecordsFoundOaiResponse(OAIPMH oaipmh, Request request) {
    oaipmh.withErrors(createNoRecordsFoundError());
    return getResponseHelper().buildFailureResponse(oaipmh, request);
  }

  protected javax.ws.rs.core.Response buildResponse(OAIPMH oai, Request request) {
    if (!oai.getErrors().isEmpty()) {
      return getResponseHelper().buildFailureResponse(oai, request);
    }
    return getResponseHelper().buildSuccessResponse(oai);
  }

  protected void handleException(Promise<Response> promise, Throwable e) {
    logger.error(GENERIC_ERROR_MESSAGE, e);
    promise.fail(e);
  }

  protected abstract List<OAIPMHerrorType> validateRequest(Request request);

  protected void addRecordsToOaiResponse(OAIPMH oaipmh, Collection<RecordType> records) {
    if (!records.isEmpty()) {
      logger.debug("{} records found for the request.", records.size());
      oaipmh.withListRecords(new ListRecordsType().withRecords(records));
    } else {
      oaipmh.withErrors(createNoRecordsFoundError());
    }
  }

  protected abstract void addResumptionTokenToOaiResponse(OAIPMH oaipmh, ResumptionTokenType resumptionToken);

}
