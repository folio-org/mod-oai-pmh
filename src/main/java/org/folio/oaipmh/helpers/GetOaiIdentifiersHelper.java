package org.folio.oaipmh.helpers;

import static me.escoffier.vertx.completablefuture.VertxCompletableFuture.supplyBlockingAsync;
import static org.folio.oaipmh.Constants.LIST_ILLEGAL_ARGUMENTS_ERROR;
import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FLOW_ERROR;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.isDeletedRecordsEnabled;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.rest.tools.client.Response;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.openarchives.oai._2.ListIdentifiersType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;

import io.vertx.core.Context;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;

public class GetOaiIdentifiersHelper extends AbstractHelper {

  private static final Logger logger = LoggerFactory.getLogger(GetOaiIdentifiersHelper.class);
  private static final String GENERIC_ERROR = "Error happened while processing ListIdentifiers verb request";

  @Override
  public CompletableFuture<javax.ws.rs.core.Response> handle(Request request, Context ctx) {
    CompletableFuture<javax.ws.rs.core.Response> future = new VertxCompletableFuture<>(ctx);
    try {
      ResponseHelper responseHelper = getResponseHelper();
      // 1. Restore request from resumptionToken if present
      if (request.getResumptionToken() != null && !request.restoreFromResumptionToken()) {
        OAIPMH oai = responseHelper.buildOaipmhResponseWithErrors(request, BAD_ARGUMENT, LIST_ILLEGAL_ARGUMENTS_ERROR);
        future.complete(getResponseHelper().buildFailureResponse(oai, request));
        return future;
      }

      // 2. Validate request
      List<OAIPMHerrorType> errors = validateListRequest(request);
      if (!errors.isEmpty()) {
        OAIPMH oai;
        if (request.isRestored()) {
          oai = responseHelper.buildOaipmhResponseWithErrors(request, BAD_RESUMPTION_TOKEN, RESUMPTION_TOKEN_FORMAT_ERROR);
        } else {
          oai = responseHelper.buildOaipmhResponseWithErrors(request, errors);
        }
        future.complete(getResponseHelper().buildFailureResponse(oai, request));
        return future;
      }

      HttpClientInterface httpClient = getOkapiClient(request.getOkapiHeaders());
      final String instanceEndpoint = storageHelper.buildRecordsEndpoint(request, isDeletedRecordsEnabled(request, REPOSITORY_DELETED_RECORDS));

      // 3. Search for instances
      VertxCompletableFuture.from(ctx, httpClient.request(instanceEndpoint, request.getOkapiHeaders(), false))
        // 4. Verify response and build list of identifiers
        .thenApply(response -> buildListIdentifiers(request, response))
        // 5. Build final response to client (potentially blocking operation thus running on worker thread)
        .thenCompose(oai -> supplyBlockingAsync(ctx, () -> buildResponse(oai, request)))
        .thenAccept(future::complete)
        .exceptionally(e -> {
          logger.error(GENERIC_ERROR, e);
          future.completeExceptionally(e);
          return null;
        });
    } catch (Exception e) {
      logger.error(GENERIC_ERROR, e);
      future.completeExceptionally(e);
    }

    return future;
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
  private OAIPMH buildListIdentifiers(Request request, Response instancesResponse) {
    if (!Response.isSuccess(instancesResponse.getCode())) {
      logger.error("No instances found. Service responded with error: " + instancesResponse.getError());
      // The storage service could not return instances so we have to send 500 back to client
      throw new IllegalStateException(instancesResponse.getError().toString());
    }

    ResponseHelper responseHelper = getResponseHelper();
    JsonArray instances;
    if (isDeletedRecordsEnabled(request, REPOSITORY_DELETED_RECORDS)) {
      instances = storageHelper.getRecordsItems(instancesResponse.getBody());
    } else {
      instances = storageHelper.getItems(instancesResponse.getBody());
    }
    Integer totalRecords = storageHelper.getTotalRecords(instancesResponse.getBody());
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
