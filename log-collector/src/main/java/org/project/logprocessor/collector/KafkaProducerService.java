package org.project.logprocessor.collector;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {
  private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
  private static final String TOPIC_NAME = "log-events";

  @Autowired private KafkaTemplate<String, LogEvent> kafkaTemplate;
  private final AtomicLong sentEvents = new AtomicLong(0);
  private final AtomicLong failedEvents = new AtomicLong(0);

  public void sendLogEvent(LogEvent logEvent) {
    try {
      CompletableFuture<SendResult<String, LogEvent>> future =
          kafkaTemplate.send(TOPIC_NAME, logEvent.getId(), logEvent);
      future.whenComplete(
          (result, throwable) -> {
            if (throwable != null) {
              logger.error("Failed to send log event: {}", logEvent.getId());
              failedEvents.incrementAndGet();
            } else {
              logger.debug("Successfully sent log event: {}", logEvent.getId());
              sentEvents.incrementAndGet();
            }
          });
    } catch (Exception ex) {
      logger.error("Error sending log event to kafka", ex);
      throw ex;
    }
  }

  public void fallbackSendLogEvent(LogEvent logEvent, Exception ex) {
    logger.warn(
        "Circuit breaker activated for log event: {}. Reason: {}",
        logEvent.getId(),
        ex.getMessage());
    failedEvents.incrementAndGet();
  }

  public long getSentEventsCount() {
    return sentEvents.get();
  }

  public long getFailedEventsCount() {
    return failedEvents.get();
  }
}
