package org.folio.oaipmh.common;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.junit5.VertxTestContext;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import javax.ws.rs.NotFoundException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;

public abstract class AbstractInstancesTest {

  protected static final int INSTANCES_EXPIRATION_TIME_IN_SECONDS = 86400;

  protected static final String COMMON_JSON = "{\"instanceId\": \"00000000-0000-4000-a000-000000000000\", \"source\": \"FOLIO\", \"updatedDate\": \"2020-06-15T11:07:48.563Z\",  \"deleted\": \"false\",  \"suppressFromDiscovery\": \"false\"}";

  protected static final String EXPIRED_REQUEST_ID = UUID.randomUUID()
    .toString();
  protected static final String REQUEST_ID = UUID.randomUUID()
    .toString();
  protected static final String NON_EXISTENT_REQUEST_ID = UUID.randomUUID()
    .toString();

  protected static final String EXPIRED_INSTANCE_ID = UUID.randomUUID()
    .toString();
  protected static final String INSTANCE_ID = UUID.randomUUID()
    .toString();
  protected static final String NON_EXISTENT_INSTANCE_ID = UUID.randomUUID()
    .toString();

  protected List<String> requestIds = List.of(EXPIRED_REQUEST_ID, REQUEST_ID);
  protected List<String> instancesIds = List.of(EXPIRED_INSTANCE_ID, INSTANCE_ID);
  protected List<String> nonExistentInstancesIds = List.of(NON_EXISTENT_INSTANCE_ID);

  protected static final OffsetDateTime notExpiredDate = OffsetDateTime.now(ZoneId.systemDefault());
  protected static final OffsetDateTime expiredDate = OffsetDateTime.now(ZoneId.systemDefault())
    .minusSeconds(INSTANCES_EXPIRATION_TIME_IN_SECONDS);

  protected static final RequestMetadataLb expiredRequestMetadata = new RequestMetadataLb()
    .setRequestId(UUID.fromString(EXPIRED_REQUEST_ID))
    .setLastUpdatedDate(expiredDate);
  protected static final RequestMetadataLb requestMetadata = new RequestMetadataLb().setRequestId(UUID.fromString(REQUEST_ID))
    .setLastUpdatedDate(notExpiredDate);
  protected static final RequestMetadataLb nonExistentRequestMetadata = new RequestMetadataLb()
    .setRequestId(UUID.fromString(NON_EXISTENT_REQUEST_ID))
    .setLastUpdatedDate(notExpiredDate);

  protected static final Instances instance_1 = new Instances().setInstanceId(UUID.fromString(EXPIRED_INSTANCE_ID))
    .setRequestId(UUID.fromString(EXPIRED_REQUEST_ID));
  protected static final Instances instance_2 = new Instances().setInstanceId(UUID.fromString(INSTANCE_ID))
    .setRequestId(UUID.fromString(REQUEST_ID));

  protected static final List<Instances> instancesList = List.of(instance_1, instance_2);
  protected static final List<RequestMetadataLb> requestMetadataList = List.of(expiredRequestMetadata, requestMetadata);

  @BeforeEach
  void setup(VertxTestContext testContext) {
    List<Future> saveRequestMetadataFutures = new ArrayList<>();
    requestMetadataList
      .forEach(elem -> saveRequestMetadataFutures.add(getInstancesDao().saveRequestMetadata(elem, OAI_TEST_TENANT)));

    GenericCompositeFuture.all(saveRequestMetadataFutures)
      .onSuccess(v -> getInstancesDao().saveInstances(instancesList, OAI_TEST_TENANT)
        .onSuccess(e -> testContext.completeNow())
        .onFailure(testContext::failNow))
      .onFailure(testContext::failNow);
  }

  @AfterEach
  void cleanUp(VertxTestContext testContext) {
    cleanData().onSuccess(v -> testContext.completeNow())
      .onFailure(testContext::failNow);
  }

  protected Future<Void> cleanData() {
    Promise<Void> promise = Promise.promise();
    List<Future> futures = new ArrayList<>();
    requestIds.forEach(elem -> futures.add(getInstancesDao().deleteRequestMetadataByRequestId(elem, OAI_TEST_TENANT)));

    GenericCompositeFuture.all(futures)
      .onSuccess(v -> promise.complete())
      .onFailure(throwable -> {
        if (throwable instanceof NotFoundException) {
          promise.complete();
        } else {
          promise.fail(throwable);
        }
      });
    return promise.future();
  }

  protected abstract InstancesDao getInstancesDao();

}
