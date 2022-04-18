package org.folio.oaipmh.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicInteger;

@Getter
@Setter
@NoArgsConstructor
public class StatisticsHolder {
    final AtomicInteger returnedInstancesCounter = new AtomicInteger(0);
    final AtomicInteger skippedInstancesCounter = new AtomicInteger(0);
    final AtomicInteger failedInstancesCounter = new AtomicInteger(0);
    final AtomicInteger supressedFromDiscoveryCounter = new AtomicInteger(0);
}
