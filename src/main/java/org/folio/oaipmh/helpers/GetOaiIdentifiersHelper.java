package org.folio.oaipmh.helpers;

import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FLOW_ERROR;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

import java.util.Date;
import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.rest.client.SourceStorageSourceRecordsClient;
import org.openarchives.oai._2.ListIdentifiersType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class GetOaiIdentifiersHelper extends AbstractHelper {

  private static final Logger logger = LoggerFactory.getLogger(GetOaiIdentifiersHelper.class);
  private static final String GENERIC_ERROR = "Error happened while processing ListIdentifiers verb request";

  @Override
  public Future<javax.ws.rs.core.Response> handle(Request request, Context ctx) {
    Promise<javax.ws.rs.core.Response> promise = Promise.promise();
    try {
      ResponseHelper responseHelper = getResponseHelper();
      // 1. Validate request
      List<OAIPMHerrorType> errors = validateListRequest(request);
      if (!errors.isEmpty()) {
        OAIPMH oai;
        if (request.isRestored()) {
          oai = responseHelper.buildOaipmhResponseWithErrors(request, BAD_RESUMPTION_TOKEN, RESUMPTION_TOKEN_FORMAT_ERROR);
        } else {
          oai = responseHelper.buildOaipmhResponseWithErrors(request, errors);
        }
        promise.complete(getResponseHelper().buildFailureResponse(oai, request));
        return promise.future();
      }

      final SourceStorageSourceRecordsClient srsClient = new SourceStorageSourceRecordsClient(request.getOkapiUrl(),
        request.getTenant(), request.getOkapiToken());

      final boolean deletedRecordsSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request);
      final boolean suppressedRecordsSupport = getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

      final Date updatedAfter = request.getFrom() == null ? null : convertStringToDate(request.getFrom(), false);
      final Date updatedBefore = request.getUntil() == null ? null : convertStringToDate(request.getUntil(), true);

      int batchSize = Integer.parseInt(
        RepositoryConfigurationUtil.getProperty(request.getTenant(),
          REPOSITORY_MAX_RECORDS_PER_RESPONSE));
      //source-storage/sourceRecords?query=recordType%3D%3DMARC+and+externalIdsHolder.instanceId%3D%3D6eee8eb9-db1a-46e2-a8ad-780f19974efa&limit=51&offset=0
      srsClient.getSourceStorageSourceRecords(
        null,
        null,
        null,
        "MARC",
        //NULL if we want suppressed and not suppressed, TRUE = ONLY SUPPRESSED FALSE = ONLY NOT SUPPRESSED
        !suppressedRecordsSupport ? suppressedRecordsSupport : null,
        deletedRecordsSupport,
        null,
        updatedAfter,
        updatedBefore,
        null,
        request.getOffset(),
        batchSize,
        response -> response.bodyHandler(bh -> {
          try {
            final OAIPMH oaipmh = buildListIdentifiers(request, bh.toJsonObject());
            promise.complete(buildResponse(oaipmh, request));
          } catch (Exception e) {
            logger.error("Exception getting list of identifiers", e);
            promise.fail(e);
          }
        }));


//      //region REMOVE IT
//      // 2. Search for instances
//       VertxCompletableFuture.from(ctx, httpClient.request(instanceEndpoint, request.getOkapiHeaders(), false))
//        // 3. Verify response and build list of identifiers
//        .thenApply(response -> buildListIdentifiers(request, response))
//        // 4. Build final response to client (potentially blocking operation thus running on worker thread)
//        .thenCompose(oai -> supplyBlockingAsync(ctx, () -> buildResponse(oai, request)))
//        .thenAccept(promise::complete)
//        .exceptionally(e -> {
//          logger.error(GENERIC_ERROR, e);
//          promise.fail(e);
//          return null;
//        });
    } catch (Exception e) {
      logger.error(GENERIC_ERROR, e);
      promise.fail(e);
    }
    //endregion
    return promise.future();
  }

  /**
   * Check if there are identifiers built and construct success response, otherwise return response with error(s)
   */
  private javax.ws.rs.core.Response buildResponse(OAIPMH oai, Request request) {
    if (oai.getListIdentifiers() == null) {
      return getResponseHelper().buildFailureResponse(oai, request);
    } else {
      return getResponseHelper().buildSuccessResponse(oai);
    }
  }

  /**
   * Builds {@link ListIdentifiersType} with headers if there is any item or {@code null}
   *
   * @param request           request
   * @param instancesResponse the response from the storage which contains items
   * @return {@link ListIdentifiersType} with headers if there is any or {@code null}
   */
  private OAIPMH buildListIdentifiers(Request request, JsonObject instancesResponse) {

    ResponseHelper responseHelper = getResponseHelper();
    JsonArray instances = storageHelper.getItems(instancesResponse);
    Integer totalRecords = storageHelper.getTotalRecords(instancesResponse);
    if (request.isRestored() && !canResumeRequestSequence(request, totalRecords, instances)) {
      return responseHelper.buildOaipmhResponseWithErrors(request, BAD_RESUMPTION_TOKEN, RESUMPTION_TOKEN_FLOW_ERROR);
    }
    if (instances != null && !instances.isEmpty()) {
      logger.debug("{} entries retrieved out of {}", instances.size(), totalRecords);

      ListIdentifiersType identifiers = new ListIdentifiersType()
        .withResumptionToken(buildResumptionToken(request, instances, totalRecords));

      String identifierPrefix = request.getIdentifierPrefix();
      instances.stream()
        .map(object -> (JsonObject) object)
        .filter(instance -> StringUtils.isNotEmpty(storageHelper.getIdentifierId(instance)))
        .filter(instance -> filterInstance(request, instance))
        .map(instance -> addHeader(identifierPrefix, request, instance))
        .forEach(identifiers::withHeaders);

      if (identifiers.getHeaders().isEmpty()) {
        OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request);
        return oaipmh.withErrors(createNoRecordsFoundError());
      }

      return responseHelper.buildBaseOaipmhResponse(request).withListIdentifiers(identifiers);
    }
    return responseHelper.buildOaipmhResponseWithErrors(request, createNoRecordsFoundError());
  }

}
