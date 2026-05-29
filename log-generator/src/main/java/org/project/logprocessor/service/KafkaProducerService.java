package org.project.logprocessor.service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.project.logprocessor.model.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Service
public class KafkaProducerService {

  private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

  private final KafkaTemplate<String, String> kafkaTemplate;

  private final ObjectMapper objectMapper;
  private final Counter sentCounter;
  private final Counter failedCounter;
  private final Timer sendTimer;

  @Value("${app.kafka.topic:log-events}")
  private String topicName;

  @Autowired
  public KafkaProducerService(
      KafkaTemplate<String, String> kafkaTemplate,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry) {
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
    this.sentCounter =
        Counter.builder("kafka_messages_sent")
            .description("Number of messages sent to Kafka")
            .register(meterRegistry);
    this.failedCounter =
        Counter.builder("kafka_messages_failed")
            .description("Number of failed Kafka sends")
            .register(meterRegistry);
    this.sendTimer =
        Timer.builder("Kafka_send_duration")
            .description("Time taken to send messages to Kafka")
            .register(meterRegistry);
  }

  public CompletableFuture<Void> sendLogEvent(LogEvent logEvent) {
    try {
      return sendTimer.recordCallable(
          () -> {
            try {
              String eventJson = objectMapper.writeValueAsString(logEvent);
              CompletableFuture<SendResult<String, String>> future =
                  kafkaTemplate
                      .send(topicName, logEvent.getCorrelationId(), eventJson)
                      .toCompletableFuture();
              return future.handle(
                  (result, throwable) -> {
                    if (throwable != null) {
                      failedCounter.increment();
                      logger.error("Failed to send log event: {}", throwable.getMessage());
                      throw new RuntimeException(throwable);
                    } else {
                      sentCounter.increment();
                      logger.debug(
                          "Sent log event with correlation ID: {}", logEvent.getCorrelationId());
                      return null;
                    }
                  });
            } catch (Exception e) {
              failedCounter.increment();
              throw new RuntimeException("Failed to serialize log event", e);
            }
          });
    } catch (Exception e) {
      failedCounter.increment();
      CompletableFuture<Void> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(e);
      return failedFuture;
    }
  }

  public CompletableFuture<Void> sendLogEventsBatch(List<LogEvent> logEvents) {
    Timer.Sample sample = Timer.start();
    List<CompletableFuture<SendResult<String, String>>> futures =
        logEvents.stream()
            .map(
                logEvent -> {
                  try {
                    String eventJson = objectMapper.writeValueAsString(logEvent);
                    return kafkaTemplate
                        .send(topicName, logEvent.getCorrelationId(), eventJson)
                        .toCompletableFuture();
                  } catch (JsonProcessingException e) {
                    failedCounter.increment();
                    CompletableFuture<SendResult<String, String>> failedFuture =
                        new CompletableFuture<>();
                    failedFuture.completeExceptionally(e);
                    return failedFuture;
                  }
                })
            .toList();

    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .handle(
            (result, throwable) -> {
              sample.stop(sendTimer);
              if (throwable != null) {
                logger.error("Batch send failed: {}", throwable.getMessage());
                throw new RuntimeException(throwable);
              } else {
                sentCounter.increment();
                logger.debug("Sent batch of {} log events", logEvents.size());
                return null;
              }
            });
  }
}
