package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseHelper;
import org.folio.rest.tools.client.Response;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.openarchives.oai._2.ListIdentifiersType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.ResumptionTokenType;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.folio.oaipmh.Constants.LIST_ILLEGAL_ARGUMENTS_ERROR;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FLOW_ERROR;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.folio.rest.jaxrs.resource.Oai.GetOaiIdentifiersResponse;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;

public class GetOaiIdentifiersHelper extends AbstractHelper {

  private static final Logger logger = LoggerFactory.getLogger(GetOaiIdentifiersHelper.class);
  private static final String GENERIC_ERROR = "Error happened while processing ListIdentifiers verb request";

  @Override
  public CompletableFuture<javax.ws.rs.core.Response> handle(Request request, Context ctx) {
    CompletableFuture<javax.ws.rs.core.Response> future = new VertxCompletableFuture<>(ctx);
    try {
      // 1. Restore request from resumptionToken if present
      if (request.getResumptionToken() != null && !request.restoreFromResumptionToken()) {
          OAIPMH oai = buildBaseResponse(request)
            .withErrors(new OAIPMHerrorType().withCode(BAD_ARGUMENT).withValue(LIST_ILLEGAL_ARGUMENTS_ERROR));
          future.complete(buildNoRecordsResponse(oai));
          return future;
      }

      // 2. Validate request
      List<OAIPMHerrorType> errors = validateListRequest(request);
      if (!errors.isEmpty()) {
        OAIPMH oai = buildBaseResponse(request);
        if (request.isRestored()) {
          oai.withErrors(new OAIPMHerrorType().withCode(BAD_RESUMPTION_TOKEN).withValue(RESUMPTION_TOKEN_FORMAT_ERROR));
        } else {
          oai.withErrors(errors);
        }
        future.complete(buildNoRecordsResponse(oai));
        return future;
      }

      HttpClientInterface httpClient = getOkapiClient(request.getOkapiHeaders());

      // 3. Search for instances
      VertxCompletableFuture.from(ctx, httpClient.request(storageHelper.buildItemsEndpoint(request), request.getOkapiHeaders(), false))
        // 4. Verify response and build list of identifiers
        .thenApply(response -> buildListIdentifiers(request, response))
        // 5. Build final response to client (potentially blocking operation thus running on worker thread)
        .thenCompose(oai -> supplyBlockingAsync(ctx, () -> buildResponse(oai))
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
  private javax.ws.rs.core.Response buildResponse(OAIPMH oai) {
    if (oai.getListIdentifiers() == null) {
      return buildNoRecordsResponse(oai);
    } else {
      return buildSuccessResponse(oai);
    }
  }

  private javax.ws.rs.core.Response buildNoRecordsResponse(OAIPMH oai) {
    String responseBody = ResponseHelper.getInstance().writeToString(oai);

    // According to oai-pmh.raml the service will return different http codes depending on the error
    Set<OAIPMHerrorcodeType> errorCodes = getErrorCodes(oai);
    if (errorCodes.contains(BAD_ARGUMENT) || errorCodes.contains(BAD_RESUMPTION_TOKEN)) {
      return GetOaiIdentifiersResponse.respond400WithApplicationXml(responseBody);
    } else if (errorCodes.contains(CANNOT_DISSEMINATE_FORMAT)) {
      return GetOaiIdentifiersResponse.respond422WithApplicationXml(responseBody);
    }
    return GetOaiIdentifiersResponse.respond404WithApplicationXml(responseBody);
  }

  private javax.ws.rs.core.Response buildSuccessResponse(OAIPMH oai) {
    return GetOaiIdentifiersResponse.respond200WithApplicationXml(ResponseHelper.getInstance().writeToString(oai));
  }
  /**
   * Builds {@link ListIdentifiersType} with headers if there is any item or {@code null}
   * @param request request
   * @param instancesResponse the response from the storage which contains items
   * @return {@link ListIdentifiersType} with headers if there is any or {@code null}
   */
  private OAIPMH buildListIdentifiers(Request request, Response instancesResponse) {
    if (!Response.isSuccess(instancesResponse.getCode())) {
      logger.error("No instances found. Service responded with error: " + instancesResponse.getError());
      // The storage service could not return instances so we have to send 500 back to client
      throw new IllegalStateException(instancesResponse.getError().toString());
    }

    JsonArray instances = storageHelper.getItems(instancesResponse.getBody());
    Integer totalRecords = storageHelper.getTotalRecords(instancesResponse.getBody());
    if (request.isRestored() && !canResumeRequestSequence(request, totalRecords, instances)) {
        return buildBaseResponse(request).withErrors(new OAIPMHerrorType()
        .withCode(BAD_RESUMPTION_TOKEN)
        .withValue(RESUMPTION_TOKEN_FLOW_ERROR));
    }
    if (instances != null && !instances.isEmpty()) {
      ListIdentifiersType identifiers = new ListIdentifiersType();

      String resumptionToken = buildResumptionToken(request, instances, totalRecords);
      if (resumptionToken != null) {
        identifiers.withResumptionToken(new ResumptionTokenType()
          .withValue(resumptionToken)
          .withCompleteListSize(BigInteger.valueOf(totalRecords))
          .withCursor(request.getOffset() == 0 ? BigInteger.ZERO : BigInteger.valueOf(request.getOffset())));
      }

      String identifierPrefix = request.getIdentifierPrefix();
      instances.stream()
        .map(instance -> populateHeader(identifierPrefix, (JsonObject) instance))
        .forEach(identifiers::withHeaders);

      return buildBaseResponse(request).withListIdentifiers(identifiers);
    }

    return buildBaseResponse(request).withErrors(createNoRecordsFoundError());
  }
}
