package org.folio.oaipmh.common;

import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.junit5.VertxTestContext;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.NotFoundException;
import org.folio.oaipmh.dao.ErrorsDao;
import org.folio.oaipmh.dao.InstancesDao;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.jooq.tables.pojos.Errors;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractErrorsTest {

  protected static final String NULL_REQUEST_ID = null;
  protected static final String INSTANCE_ID = UUID.randomUUID().toString();
  protected static final UUID REQUEST_ID = UUID.randomUUID();
  protected static final OffsetDateTime notExpiredDate = OffsetDateTime
      .now(ZoneId.systemDefault());
  protected static final Errors error = new Errors().setId(1L).setInstanceId(INSTANCE_ID)
      .setRequestId(REQUEST_ID).setErrorMsg("some error msg");
  protected static final RequestMetadataLb requestMetadata = new RequestMetadataLb()
      .setRequestId(REQUEST_ID)
      .setLastUpdatedDate(notExpiredDate).setStartedDate(notExpiredDate);
  protected static final List<Errors> errorList = List.of(error);
  protected static final List<RequestMetadataLb> requestMetadataList = List.of(requestMetadata);

  protected List<String> requestIds = List.of(REQUEST_ID.toString());

  @BeforeEach
  void setup(VertxTestContext testContext) {
    List<Future> saveRequestMetadataFutures = new ArrayList<>();
    requestMetadataList
        .forEach(elem -> saveRequestMetadataFutures
            .add(getInstancesDao().saveRequestMetadata(elem, OAI_TEST_TENANT)));

    List<Future> saveErrorsFutures = new ArrayList<>();
    errorList.forEach(err ->
        saveErrorsFutures.add(getErrorsDao().saveErrors(err, OAI_TEST_TENANT)));

    GenericCompositeFuture.all(saveRequestMetadataFutures)
        .onSuccess(v -> GenericCompositeFuture.all(saveErrorsFutures)
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
    requestIds.forEach(elem ->
        futures.add(getInstancesDao().deleteRequestMetadataByRequestId(elem, OAI_TEST_TENANT)));

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

  protected abstract ErrorsDao getErrorsDao();

  protected abstract InstancesDao getInstancesDao();
}
