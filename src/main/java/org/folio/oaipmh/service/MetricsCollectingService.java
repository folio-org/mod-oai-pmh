package org.folio.oaipmh.service;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.groupingBy;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.With;
import lombok.extern.log4j.Log4j2;

@EnableScheduling
@Log4j2
@Configuration
public class MetricsCollectingService {

  private static final int METRICS_COLLECTION_DELAY_IN_MS = 60 * 1000;

  @Getter
  @Setter
  @AllArgsConstructor
  @NoArgsConstructor
  private static class Hit {
    @With
    private long start;
    @With
    private long end;
  }

  public enum MetricOperation {
    PARSE_XML, INVENTORY_STORAGE_RESPONSE, SRS_RESPONSE, INSTANCES_PROCESSING, SEND_REQUEST
  }

  private static final ConcurrentMap<String, Hit> hits = new ConcurrentHashMap<>();

  private static class Holder {
    private static final MetricsCollectingService service = new MetricsCollectingService();
  }

  public static MetricsCollectingService getInstance() {
    return Holder.service;
  }

  @Scheduled(fixedDelay = METRICS_COLLECTION_DELAY_IN_MS, initialDelay = METRICS_COLLECTION_DELAY_IN_MS)
  public Map<String, DoubleSummaryStatistics> scheduledMetricsCollectionTask() {
    Map<String, DoubleSummaryStatistics> statistics = null;
    if (log.isDebugEnabled()) {
      if (!hits.isEmpty()) {
        statistics = hits.entrySet()
          .stream()
          .filter(e -> e.getValue().getEnd() != 0)
          .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), hits.remove(e.getKey())))
          .collect(groupingBy(e -> e.getKey().split("\\|")[0], Collectors.summarizingDouble(this::difference)));
        if (!statistics.isEmpty()) {
          log.debug("----------------- {} -----------------", LocalDateTime.now()
            .format(DateTimeFormatter.ISO_DATE_TIME));
          statistics.forEach((operation, statistic) -> log.debug("{}  ---> Avg. time: {} ms, min: {} ms, max: {} ms, cnt: {}",
              operation, String.format("%.1f", statistic.getAverage()), statistic.getMin(), statistic.getMax(), statistic.getCount()));
        }
      }
      if (!hits.isEmpty()) {
        log.debug("Metrics scheduler, unpaired hits size: {}", hits.size());
      }
    }
    return statistics;
  }

  public void startMetric(String requestId, MetricOperation operation) {
    if (log.isDebugEnabled()) {
      hits.put(buildKey(requestId, operation), new Hit().withStart(currentTimeMillis()));
    }
  }

  public void endMetric(String requestId, MetricOperation operation) {
    if (log.isDebugEnabled()) {
      var key = buildKey(requestId, operation);
      hits.replace(key, hits.get(key).withEnd(currentTimeMillis()));
    }
  }

  public void reset() {
    hits.clear();
  }

  private long difference(AbstractMap.SimpleEntry<String, Hit> hit) {
    return hit.getValue().getEnd() - hit.getValue().getStart();
  }

  private String buildKey(String requestId, MetricOperation operation) {
    return format("%s|%s", operation.name(), requestId);
  }
}
