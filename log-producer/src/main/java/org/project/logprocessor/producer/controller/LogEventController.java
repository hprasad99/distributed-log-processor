package org.project.logprocessor.producer.controller;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.UUID;
import org.project.logprocessor.producer.model.LogEvent;
import org.project.logprocessor.producer.service.KafkaProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/logs")
public class LogEventController {

  private static final Logger logger = LoggerFactory.getLogger(LogEventController.class);

  @Autowired private KafkaProducerService kafkaProducerService;

  private final Counter logCounter;

  public LogEventController(MeterRegistry meterRegistry) {
    this.logCounter =
        Counter.builder("log_events_received_total")
            .description("Total number of log events received")
            .register(meterRegistry);
  }

  @PostMapping
  @Timed(value = "log_event_processing_time", description = "Time taken to process log event")
  public ResponseEntity<Map<String, String>> createLogEvent(@RequestBody LogEvent logEvent) {
    try {
      if (logEvent.getId() == null) {
        logEvent.setId(String.valueOf(UUID.randomUUID()));
      }

      kafkaProducerService.sendLogEvent(logEvent);
      logCounter.increment();
      logger.info("Log event created successfully: {}", logEvent.getId());
      return ResponseEntity.ok(
          Map.of(
              "status",
              "success",
              "id",
              logEvent.getId(),
              "message",
              "Log event queued for processing"));
    } catch (Exception ex) {
      logger.error("Failed to process log event", ex);
      return ResponseEntity.internalServerError()
          .body(Map.of("status", "error", "message", "Failed to process log event"));
    }
  }
}
