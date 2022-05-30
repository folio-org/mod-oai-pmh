package org.folio.oaipmh.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@NoArgsConstructor
@Getter
public class StatisticsHolder {

  final AtomicInteger downloadedAndSavedInstancesCounter = new AtomicInteger();
  final AtomicInteger failedToSaveInstancesCounter = new AtomicInteger();
  final AtomicInteger returnedInstancesCounter = new AtomicInteger();
  final AtomicInteger skippedInstancesCounter = new AtomicInteger();
  final AtomicInteger failedInstancesCounter = new AtomicInteger();
  final AtomicInteger suppressedFromDiscoveryCounter = new AtomicInteger();

  final CopyOnWriteArrayList<String> failedToSaveInstancesIds = new CopyOnWriteArrayList<>();
  final CopyOnWriteArrayList<String> failedInstancesIds = new CopyOnWriteArrayList<>();
  final CopyOnWriteArrayList<String> skippedInstancesIds = new CopyOnWriteArrayList<>();
  final CopyOnWriteArrayList<String> suppressedInstancesIds = new CopyOnWriteArrayList<>();

  public int addDownloadedAndSavedInstancesCounter(int delta) {
    return downloadedAndSavedInstancesCounter.addAndGet(delta);
  }

  public int addFailedToSaveInstancesCounter(int delta) {
    return failedToSaveInstancesCounter.addAndGet(delta);
  }

  public int addReturnedInstancesCounter(int delta) {
    return returnedInstancesCounter.addAndGet(delta);
  }

  public int addFailedInstancesCounter(int delta) {
    return failedInstancesCounter.addAndGet(delta);
  }

  public int addSkippedInstancesCounter(int delta) {
    return skippedInstancesCounter.addAndGet(delta);
  }

  public int addSuppressedInstancesCounter(int delta) {
    return suppressedFromDiscoveryCounter.addAndGet(delta);
  }

  public void addFailedToSaveInstancesIds(List<String> uuids) {
    failedToSaveInstancesIds.addAll(uuids);
  }

  public boolean addFailedInstancesIds(String... uuids) {
    return failedInstancesIds.addAll(Arrays.asList(uuids));
  }

  public boolean addSkippedInstancesIds(String... uuids) {
    return skippedInstancesIds.addAll(Arrays.asList(uuids));
  }

  public boolean addSuppressedInstancesIds(String... uuids) {
    return suppressedInstancesIds.addAll(Arrays.asList(uuids));
  }
}
