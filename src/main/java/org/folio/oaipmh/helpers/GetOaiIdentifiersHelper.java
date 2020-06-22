package org.folio.oaipmh.helpers;

import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FLOW_ERROR;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
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
      //TODO: returns query string (parts in [] are optional, depending on the properties of tenant|env, @name - it's an attribute of request etc.):
      // ?query=recordType==MARC [and additionalInfo.suppressDiscovery==false]
      // [and externalIdsHolder.instanceId==@identifier] [and metadata.updatedDate<UNTIL_DATE_STR]
      // [and metadata.updatedDate>=FROM_DATA_STR and metadata.updatedDate<UNTIL_DATE_STR]
      //TODO SHOULD SUPPRESS FROM DISCOVERY BE TAKEN FROM CONFIG?
      //TODO ADD DELETED RECORD SUPPORT WHEN IT'S READY
      //TODO OFFSET & LIMIT???
      //TODO REVIEW BY FOLIJET
      final Date updatedAfter = request.getFrom() == null ? null : convertStringToDate(request.getFrom());
      final Date updatedBefore = request.getUntil() == null ? null : convertStringToDate(request.getUntil());

      srsClient.getSourceStorageSourceRecords(
        null,
        null,
        null,
        null,
        null,
        false,
        null,
        updatedAfter,
        updatedBefore,
        null,
        0,
        Integer.MAX_VALUE,
        response -> response.bodyHandler(bh -> {
          try {
            final OAIPMH oaipmh = buildListIdentifiers(request, bh.toJsonObject());
            final Response response1 = buildResponse(oaipmh, request);
            promise.complete(response1);
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
