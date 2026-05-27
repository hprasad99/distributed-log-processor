package org.project.logprocessor.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.CompletableFuture;

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

  }
}
