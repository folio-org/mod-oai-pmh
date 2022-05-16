package org.folio.oaipmh.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicInteger;

@Getter
@Setter
@NoArgsConstructor
public class StatisticsHolder {
    final AtomicInteger downloadedAndSavedInstancesCounter = new AtomicInteger();
    final AtomicInteger failedToSaveInstancesCounter = new AtomicInteger();
    final AtomicInteger returnedInstancesCounter = new AtomicInteger();
    final AtomicInteger skippedInstancesCounter = new AtomicInteger();
    final AtomicInteger failedInstancesCounter = new AtomicInteger();
    final AtomicInteger supressedFromDiscoveryCounter = new AtomicInteger();
}
