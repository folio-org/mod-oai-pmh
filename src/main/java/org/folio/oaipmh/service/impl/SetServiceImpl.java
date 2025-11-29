package org.folio.oaipmh.service.impl;

import static java.lang.String.format;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.ILL_POLICIES;
import static org.folio.oaipmh.Constants.ILL_POLICIES_URI;
import static org.folio.oaipmh.Constants.INSTANCE_FORMATS;
import static org.folio.oaipmh.Constants.INSTANCE_FORMATS_URI;
import static org.folio.oaipmh.Constants.INSTANCE_TYPES;
import static org.folio.oaipmh.Constants.LOCATION;
import static org.folio.oaipmh.Constants.LOCATION_URI;
import static org.folio.oaipmh.Constants.MATERIAL_TYPES;
import static org.folio.oaipmh.Constants.MATERIAL_TYPES_URI;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.OKAPI_URL;
import static org.folio.oaipmh.Constants.RESOURCE_TYPES_URI;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.folio.oaipmh.WebClientProvider;
import org.folio.oaipmh.dao.SetDao;
import org.folio.oaipmh.service.SetService;

import org.folio.rest.jaxrs.model.FilteringConditionValueCollection;
import org.folio.rest.jaxrs.model.FolioSet;
import org.folio.rest.jaxrs.model.FolioSetCollection;
import org.folio.rest.jaxrs.model.SetsFilteringCondition;
import org.springframework.stereotype.Service;

@Service
public class SetServiceImpl implements SetService {

  private static final String LOCATION_JSON_FIELD_PATH = "locations";
  private static final String ILL_POLICIES_JSON_FIELD_PATH = "illPolicies";
  private static final String MATERIAL_TYPES_JSON_FIELD_PATH = "mtypes";
  private static final String INSTANCE_TYPES_JSON_FIELD_PATH = "instanceTypes";
  private static final String INSTANCE_FORMATS_JSON_FIELD_PATH = "instanceFormats";

  public static final int LOCATION_FUTURE_INDEX = 0;
  public static final int ILL_POLICIES_FUTURE_INDEX = 1;
  public static final int MATERIAL_TYPES_FUTURE_INDEX = 2;
  public static final int INSTANCE_TYPES_FUTURE_INDEX = 3;
  public static final int INSTANCE_FORMATS_FUTURE_INDEX = 4;

  private static final String NAME = "name";

  private final SetDao setDao;

  public SetServiceImpl(final SetDao setDao) {
    this.setDao = setDao;
  }

  @Override
  public Future<FolioSet> getSetById(String id, String tenantId) {
    return setDao.getSetById(id, tenantId);
  }

  @Override
  public Future<FolioSet> updateSetById(String id, FolioSet entry, String tenantId,
      String userId) {
    return setDao.updateSetById(id, entry, tenantId, userId);
  }

  @Override
  public Future<FolioSet> saveSet(FolioSet entry, String tenantId, String userId) {
    return setDao.saveSet(entry, tenantId, userId);
  }

  @Override
  public Future<Boolean> deleteSetById(String id, String tenantId) {
    return setDao.deleteSetById(id, tenantId);
  }

  @Override
  public Future<FolioSetCollection> getSetList(int offset, int limit, String tenantId) {
    return setDao.getSetList(offset, limit, tenantId);
  }

  @Override
  public Future<FilteringConditionValueCollection> getFilteringConditions(
      Map<String, String> okapiHeaders) {
    Promise<FilteringConditionValueCollection> promise = Promise.promise();
    var webClient = WebClientProvider.getWebClient();
    List<Future<JsonObject>> futures = new ArrayList<>();
    List.of(LOCATION_URI, ILL_POLICIES_URI, MATERIAL_TYPES_URI, RESOURCE_TYPES_URI,
        INSTANCE_FORMATS_URI).forEach(conditionType ->
            futures.add(getFilteringConditionValues(conditionType, webClient, okapiHeaders)));

    Future.all(futures)
        .onComplete(result -> {
          if (result.failed()) {
            promise.fail(result.cause());
          } else {
            List<SetsFilteringCondition> values = new ArrayList<>();
            values.add(jsonObjectToSetsFilteringCondition(futures.get(LOCATION_FUTURE_INDEX)
                .result(), LOCATION_JSON_FIELD_PATH, LOCATION));
            values.add(jsonObjectToSetsFilteringCondition(
                futures.get(ILL_POLICIES_FUTURE_INDEX).result(), ILL_POLICIES_JSON_FIELD_PATH,
                    ILL_POLICIES));
            values.add(jsonObjectToSetsFilteringCondition(
                futures.get(MATERIAL_TYPES_FUTURE_INDEX).result(), MATERIAL_TYPES_JSON_FIELD_PATH,
                    MATERIAL_TYPES));
            values.add(jsonObjectToSetsFilteringCondition(
                futures.get(INSTANCE_TYPES_FUTURE_INDEX).result(), INSTANCE_TYPES_JSON_FIELD_PATH,
                    INSTANCE_TYPES));
            values.add(jsonObjectToSetsFilteringCondition(
                futures.get(INSTANCE_FORMATS_FUTURE_INDEX).result(),
                    INSTANCE_FORMATS_JSON_FIELD_PATH, INSTANCE_FORMATS));

            FilteringConditionValueCollection filteringConditionValueCollection =
                new FilteringConditionValueCollection().withSetsFilteringConditions(values);
            promise.complete(filteringConditionValueCollection);
          }
        });
    return promise.future();
  }

  private Future<JsonObject> getFilteringConditionValues(String requestUri, WebClient webClient,
      Map<String, String> okapiHeaders) {
    Promise<JsonObject> promise = Promise.promise();
    requestUri = requestUri + "?" + "offset=" + 0 + "&" + "limit=" + Integer.MAX_VALUE;

    String okapiUrl = okapiHeaders.get(OKAPI_URL);
    String tenant = okapiHeaders.get(OKAPI_TENANT);
    String token = okapiHeaders.get(OKAPI_TOKEN);

    HttpRequest<Buffer> httpRequest = webClient.getAbs(okapiUrl + requestUri)
        .putHeader(OKAPI_TOKEN, token)
        .putHeader(OKAPI_TENANT, tenant)
        .putHeader(ACCEPT, APPLICATION_JSON);

    httpRequest.send().onSuccess(response -> {
      if (response.statusCode() == 200) {
        promise.complete(response.bodyAsJsonObject());
      } else {
        String msg = format("Invalid response obtained. Status code %s, message: %s.",
            response.statusCode(), response.statusMessage());
        promise.fail(new IllegalStateException(msg));
      }
    }).onFailure(promise::fail);

    return promise.future();
  }

  private SetsFilteringCondition jsonObjectToSetsFilteringCondition(JsonObject fkValues,
      String fkType, String filteringConditionName) {
    List<Object> namesList = fkValues.getJsonArray(fkType)
        .stream()
        .map(JsonObject.class::cast)
        .map(jsonObject -> jsonObject.getString(NAME))
        .collect(Collectors.toList());

    return new SetsFilteringCondition().withName(filteringConditionName)
      .withValues(namesList);
  }

}
