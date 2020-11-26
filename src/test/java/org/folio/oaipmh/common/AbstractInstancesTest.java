package org.folio.oaipmh.common;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

import org.folio.rest.jooq.tables.pojos.Instances;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.jooq.JSON;

public class AbstractInstancesTest {

  protected static final int INSTANCES_EXPIRATION_TIME_IN_SECONDS = 7200;

  protected static final String EXPIRED_REQUEST_ID = UUID.randomUUID().toString();
  protected static final String REQUEST_ID = UUID.randomUUID().toString();
  protected static final String NON_EXISTENT_REQUEST_ID = UUID.randomUUID().toString();

  protected static final String INSTANCE_ID_WITH_EXPIRED_REQUEST_ID = UUID.randomUUID().toString();
  protected static final String INSTANCE_ID_WITH_NON_EXPIRED_REQUEST_ID = UUID.randomUUID().toString();
  protected static final String NON_EXISTENT_INSTANCE_ID = UUID.randomUUID().toString();

  protected static final JSON EMPTY_JSON = JSON.valueOf("\"{}\"");

  protected List<String> requestIds = List.of(EXPIRED_REQUEST_ID, REQUEST_ID);

  protected List<String> instancesIds = List.of(INSTANCE_ID_WITH_EXPIRED_REQUEST_ID, INSTANCE_ID_WITH_EXPIRED_REQUEST_ID);
  protected List<String> nonExistentInstancesIds = List.of(NON_EXISTENT_INSTANCE_ID);

  protected static final OffsetDateTime notExpiredDate = OffsetDateTime.now(ZoneId.systemDefault());
  protected static final OffsetDateTime expiredDate = OffsetDateTime.now(ZoneId.systemDefault()).minusSeconds(INSTANCES_EXPIRATION_TIME_IN_SECONDS);

  protected static final RequestMetadataLb expiredRequestMetadata = new RequestMetadataLb().setId(UUID.fromString(EXPIRED_REQUEST_ID))
    .setRequestId(EXPIRED_REQUEST_ID)
    .setLastUpdatedDate(expiredDate);
  protected static final RequestMetadataLb requestMetadata = new RequestMetadataLb().setId(UUID.fromString(REQUEST_ID))
    .setRequestId(REQUEST_ID)
    .setLastUpdatedDate(notExpiredDate);

  protected static final Instances instance_1 = new Instances().setInstanceId(UUID.fromString(INSTANCE_ID_WITH_EXPIRED_REQUEST_ID))
    .setJson(EMPTY_JSON)
    .setRequestId(EXPIRED_REQUEST_ID);
  protected static final Instances instance_2 = new Instances().setInstanceId(UUID.fromString(INSTANCE_ID_WITH_NON_EXPIRED_REQUEST_ID))
    .setJson(EMPTY_JSON)
    .setRequestId(REQUEST_ID);

  protected static final List<Instances> instancesList = List.of(instance_1, instance_2);

}
