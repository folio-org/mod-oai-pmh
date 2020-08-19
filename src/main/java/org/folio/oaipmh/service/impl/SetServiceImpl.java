package org.folio.oaipmh.service.impl;

import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.ILL_POLICIES;
import static org.folio.oaipmh.Constants.ILL_POLICIES_URI;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.folio.oaipmh.Constants;
import org.folio.oaipmh.dao.SetDao;
import org.folio.oaipmh.service.SetService;
import org.folio.rest.jaxrs.model.FolioSet;
import org.folio.rest.jaxrs.model.FolioSetCollection;
import org.folio.rest.jaxrs.model.FilteringConditionValueCollection;
import org.folio.rest.jaxrs.model.SetsFilteringCondition;
import org.springframework.stereotype.Service;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;

@Service
public class SetServiceImpl implements SetService {

  private static final String LOCATION_JSON_FIELD_PATH = "locations";
  private static final String ILL_POLICIES_JSON_FIELD_PATH = "illPolicies";
  private static final String MATERIAL_TYPES_JSON_FIELD_PATH = "mtypes";
  private static final String INSTANCE_TYPES_JSON_FIELD_PATH = "instanceTypes";
  private static final String INSTANCE_FORMATS_JSON_FIELD_PATH = "instanceFormats";

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
  public Future<FolioSet> updateSetById(String id, FolioSet entry, String tenantId, String userId) {
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
  public Future<FilteringConditionValueCollection> getFilteringConditions(Map<String, String> okapiHeaders) {
    Promise<FilteringConditionValueCollection> promise = Promise.promise();
    HttpClient vertxHttpClient = Vertx.vertx()
      .createHttpClient();
    Future<JsonObject> locationFuture = getFilteringConditionValues(LOCATION_URI, vertxHttpClient, okapiHeaders);
    Future<JsonObject> illPoliciesFuture = getFilteringConditionValues(ILL_POLICIES_URI, vertxHttpClient, okapiHeaders);
    Future<JsonObject> materialTypesFuture = getFilteringConditionValues(MATERIAL_TYPES_URI, vertxHttpClient, okapiHeaders);
    Future<JsonObject> resourceTypeFuture = getFilteringConditionValues(RESOURCE_TYPES_URI, vertxHttpClient, okapiHeaders);
    Future<JsonObject> instanceFormatsFuture = getFilteringConditionValues(INSTANCE_FORMATS_URI, vertxHttpClient, okapiHeaders);

    CompositeFuture.all(locationFuture, illPoliciesFuture, materialTypesFuture, resourceTypeFuture, instanceFormatsFuture)
      .onComplete(result -> {
        if (result.failed()) {
          promise.fail(result.cause());
        } else {
          List<SetsFilteringCondition> values = new ArrayList<>();
          values.add(jsonObjectToSetsFilteringCondition(locationFuture.result(), LOCATION_JSON_FIELD_PATH, LOCATION));
          values.add(jsonObjectToSetsFilteringCondition(illPoliciesFuture.result(), ILL_POLICIES_JSON_FIELD_PATH, ILL_POLICIES));
          values
            .add(jsonObjectToSetsFilteringCondition(materialTypesFuture.result(), MATERIAL_TYPES_JSON_FIELD_PATH, MATERIAL_TYPES));
          values
            .add(jsonObjectToSetsFilteringCondition(resourceTypeFuture.result(), INSTANCE_TYPES_JSON_FIELD_PATH, INSTANCE_TYPES));
          values.add(jsonObjectToSetsFilteringCondition(instanceFormatsFuture.result(), INSTANCE_FORMATS_JSON_FIELD_PATH,
              Constants.INSTANCE_FORMATS));

          FilteringConditionValueCollection filteringConditionValueCollection = new FilteringConditionValueCollection()
            .withSetsFilteringConditions(values);
          promise.complete(filteringConditionValueCollection);
        }
      });
    return promise.future();
  }

  private Future<JsonObject> getFilteringConditionValues(String requestUri, HttpClient httpClient,
      Map<String, String> okapiHeaders) {
    Promise<JsonObject> promise = Promise.promise();

    String okapiUrl = okapiHeaders.get(OKAPI_URL);
    String tenant = okapiHeaders.get(OKAPI_TENANT);
    String token = okapiHeaders.get(OKAPI_TOKEN);

    HttpClientRequest httpClientRequest = httpClient.getAbs(okapiUrl.concat(requestUri))
      .putHeader(OKAPI_TOKEN, token)
      .putHeader(OKAPI_TENANT, tenant)
      .putHeader(ACCEPT, APPLICATION_JSON);

    httpClientRequest.handler(response -> {
      if (response.statusCode() == 200) {
        response.bodyHandler(body -> {
          JsonObject jsonObject = new JsonObject(body);
          promise.complete(jsonObject);
        });
      } else {
        promise.fail(new IllegalStateException(response.statusCode() + " " + response.statusMessage()));
      }
    })
      .exceptionHandler(promise::fail);
    httpClientRequest.end();

    return promise.future();
  }

  private SetsFilteringCondition jsonObjectToSetsFilteringCondition(JsonObject fkValues, String fkType,
      String filteringConditionName) {
    List<Object> namesList = fkValues.getJsonArray(fkType)
      .stream()
      .map(JsonObject.class::cast)
      .map(jsonObject -> jsonObject.getString(NAME))
      .collect(Collectors.toList());

    return new SetsFilteringCondition().withName(filteringConditionName)
      .withValues(namesList);
  }

}
