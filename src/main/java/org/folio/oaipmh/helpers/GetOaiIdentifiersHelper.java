package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.ResumptionTokenType;

import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.EMPTY;

import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_RECORDS_SOURCE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.RESUMPTION_TOKEN_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.INVENTORY;
import static org.folio.oaipmh.Constants.SRS;
import static org.folio.oaipmh.Constants.SRS_AND_INVENTORY;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getProperty;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;

public class GetOaiIdentifiersHelper extends AbstractGetRecordsHelper {

  private static final String INVENTORY_UPDATED_INSTANCES_ENDPOINT = "/inventory-hierarchy/updated-instance-ids";
  private static final String INVENTORY_UPDATED_INSTANCES_PARAMS = "?deletedRecordSupport=%s&skipSuppressedFromDiscoveryRecords=%s&onlyInstanceUpdateDate=%s%s%s";
  private static final String JSON_OBJECTS_REGEX = "(?<=\\})(?=\\{)";
  private static final String SOURCE = "source";

  @Override
  public Future<javax.ws.rs.core.Response> handle(Request request, Context ctx) {
    Promise<Response> promise = Promise.promise();
    try {
      ResponseHelper responseHelper = getResponseHelper();
      List<OAIPMHerrorType> errors = validateRequest(request);
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
      requestAndProcessInventoryRecords(request, ctx, promise);
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  @Override
  protected List<OAIPMHerrorType> validateRequest(Request request) {
    return validateListRequest(request);
  }

  @Override
  protected Future<Response> processRecords(Context ctx, Request request, JsonObject srsRecords, JsonObject inventoryRecords) {
    OAIPMH oaipmhResult = buildListIdentifiers(request, srsRecords, inventoryRecords);
    return Future.succeededFuture(buildResponse(oaipmhResult, request));
  }

  /**
   * Check if there are identifiers built and construct success response, otherwise return response with error(s)
   */
  @Override
  protected javax.ws.rs.core.Response buildResponse(OAIPMH oai, Request request) {
    if (oai.getListIdentifiers() == null) {
      return getResponseHelper().buildFailureResponse(oai, request);
    } else {
      return getResponseHelper().buildSuccessResponse(oai);
    }
  }

  @Override
  protected void addResumptionTokenToOaiResponse(OAIPMH oaipmh, ResumptionTokenType resumptionToken) {
    if (oaipmh.getListRecords() != null) {
      oaipmh.getListIdentifiers()
        .withResumptionToken(resumptionToken);
    }
  }

  @Override
  protected Future<JsonObject> requestFromInventory(Request request, int limit, List<String> listOfIds) {
    final boolean deletedRecordsSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request.getRequestId());
    final boolean suppressedRecordsSupport = getBooleanProperty(request.getRequestId(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

    var updatedAfter = request.getFrom() == null ? EMPTY :
      "&startDate=" + dateFormat.format(convertStringToDate(request.getFrom(), false, true));
    var updatedBefore = request.getUntil() == null ? EMPTY :
      "&endDate=" + dateFormat.format(convertStringToDate(request.getUntil(), true, true));

    var discoverySuppress = nonNull(deletedRecordsSupport ? null : suppressedRecordsSupport);

    var includeHoldingsAndItemsUpdatedDate = request.getMetadataPrefix().equals(MetadataPrefix.MARC21WITHHOLDINGS.getName());

    Promise<JsonObject> promise = Promise.promise();
    var params = format(INVENTORY_UPDATED_INSTANCES_PARAMS, deletedRecordsSupport,
      discoverySuppress, !includeHoldingsAndItemsUpdatedDate, updatedAfter, updatedBefore);
    String uri = request.getOkapiUrl() + INVENTORY_UPDATED_INSTANCES_ENDPOINT + params;
    processRequest(request, promise, uri, listOfIds);
    return promise.future();
  }

  @Override
  protected void handleResponse(Promise<JsonObject> promise, HttpResponse<Buffer> response, Request request) {
    int batchSize = Integer.parseInt(
      RepositoryConfigurationUtil.getProperty(request.getRequestId(),
        REPOSITORY_MAX_RECORDS_PER_RESPONSE));
    var recordsSource = getProperty(request.getRequestId(), REPOSITORY_RECORDS_SOURCE);
    var jsonStrings = isNull(response.body()) ? new String[]{} : response.bodyAsString().trim().split(JSON_OBJECTS_REGEX);
    var jsonObjects = Arrays.stream(jsonStrings)
      .map(JsonObject::new)
      .filter(json -> json.containsKey(SOURCE) && (recordsSource.equals(SRS) && json.getString(SOURCE).equals("MARC") ||
        recordsSource.equals(INVENTORY) && json.getString(SOURCE).equals("FOLIO") ||
        recordsSource.equals(SRS_AND_INVENTORY))).collect(Collectors.toList());
    var totalRecords = jsonObjects.size();
    var upperIndex = request.getOffset() + batchSize + 1;
    jsonObjects = jsonObjects.subList(request.getOffset(), upperIndex < totalRecords ? upperIndex : totalRecords);
    var jsonArr = new JsonArray();
    jsonObjects.forEach(json -> {
      json.put("metadata", new JsonObject().put("updatedDate", json.getString("updatedDate")));
      jsonArr.add(json);
    });
    var jsonInstances = new JsonObject().put("instances", jsonArr).put("totalRecords", totalRecords);
    promise.complete(jsonInstances);
  }

}
