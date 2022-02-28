package org.folio.oaipmh.service.impl;

import static org.folio.oaipmh.service.MetricsCollectingService.MetricOperation.PARSE_XML;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.UUID;

import org.folio.oaipmh.service.MetricsCollectingService;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

class MetricsCollectingServiceTest {

  private static final MetricsCollectingService service = MetricsCollectingService.getInstance();

  @Test
  void testSuccessfulMetric() {
    service.reset();
    var operationId = UUID.randomUUID().toString();
    service.startMetric(operationId, PARSE_XML);
    service.endMetric(operationId, PARSE_XML);
    var statistics = service.scheduledMetricsCollectionTask();
    assertThat(statistics.size(), Matchers.is(1));
  }

  @Test
  void testUnpairedHit() {
    service.reset();
    var operationId = UUID.randomUUID().toString();
    service.startMetric(operationId, PARSE_XML);
    var statistics = service.scheduledMetricsCollectionTask();
    assertThat(statistics.size(), Matchers.is(0));
  }
}
