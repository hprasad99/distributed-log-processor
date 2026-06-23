package org.project.logprocessor.collector;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CollectorController {
  @Autowired private LogCollectorService logCollectorService;
  @Autowired private KafkaProducerService kafkaProducerService;

  @GetMapping("/")
  public Map<String, Object> welcome() {
    return Map.of(
        "service", "Log Collector Service",
        "status", "runnning",
        "endpoints", Map.of("stats", "/api/collector/status", "health", "/actuator/health"));
  }

  @GetMapping("/api/collector/service")
  public Map<String, Object> getStats() {
    return Map.of(
        "processedEvents", logCollectorService.getProcessedEventsCount(),
        "skippedDuplicates", logCollectorService.getSkippedDuplicatesCount(),
        "sentToKafka", kafkaProducerService.getSentEventsCount(),
        "kafkaFailures", kafkaProducerService.getFailedEventsCount(),
        "status", "running");
  }
}
