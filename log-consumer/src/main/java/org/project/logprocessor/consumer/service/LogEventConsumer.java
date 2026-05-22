package org.project.logprocessor.consumer.service;

import java.time.LocalDateTime;

import org.project.logprocessor.consumer.model.LogEvent;
import org.project.logprocessor.consumer.repository.LogEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.logging.LogLevel;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import tools.jackson.databind.ObjectMapper;

@Service
public class LogEventConsumer {
  private static final Logger logger = LoggerFactory.getLogger(LogEventConsumer.class);

  @Autowired private LogEventRepository logEventRepository;

  @Autowired private ObjectMapper objectMapper;

  private final Counter processedCounter;
  private final Counter errorCounter;

  public LogEventConsumer(MeterRegistry meterRegistry) {
    this.processedCounter =
        Counter.builder("log_events_processed_total")
            .description("Total number of log events processed")
            .register(meterRegistry);
    this.errorCounter =
        Counter.builder("log_events_errors_total")
            .description("Total number of log event processing errors")
            .register(meterRegistry);
  }

  @KafkaListener(topics = "log-events", groupId = "log-consumer-group")
  @RetryableTopic(
      attempts = "3",
      backOff = @BackOff(delay = 1000, multiplier = 2.0),
      autoCreateTopics = "true",
      topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
  public void consume(
      @Payload String message,
      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
      Acknowledgment acknowledgment) {
    try {
      logger.debug("Received message from topic: {}, partition: {}", topic, partition);
      LogEvent logEvent = objectMapper.readValue(message, LogEvent.class);
      processLogEvent(logEvent);
      acknowledgment.acknowledge();
      processedCounter.increment();

      logger.debug("Successfully processed log event: {}", logEvent.getId());
    } catch (Exception e) {
      logger.error("Failed to process message: {}", message, e);
      errorCounter.increment();
      throw new RuntimeException("Failed to process log event", e);
    }
  }

  private void processLogEvent(LogEvent logEvent) {
    try {
      logEvent.setProcessadAt(LocalDateTime.now());
      logEventRepository.save(logEvent);
      if (LogLevel.ERROR.name().equals(logEvent.getLevel())) {
        handleErrorLog(logEvent);
      }

      logger.info(
          "Processed log event: {} for organization: {}",
          logEvent.getId(),
          logEvent.getOrganizationId());
    } catch (DataAccessException ex) {
      logger.error("Database error processing log event: {}", logEvent.getId(), ex);
      throw ex;
    }
  }

  private void handleErrorLog(LogEvent logEvent) {
    logger.warn("Error log detected: {} from {}", logEvent.getMessage(), logEvent.getSource());
  }
}
