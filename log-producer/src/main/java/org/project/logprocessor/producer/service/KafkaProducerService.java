package org.project.logprocessor.producer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.project.logprocessor.producer.model.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;


@Service
public class KafkaProducerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${app.kafka.topic.log-events}")
    private String logEventsTopic;

    public void sendLogEvent(LogEvent logEvent) {
        try {
            String message = objectMapper.writeValueAsString(logEvent);
            String partitionKey = logEvent.getOrganizationId();
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(logEventsTopic, partitionKey, message);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    logger.error("Failed to send log event for org={}: {}", logEvent.getOrganizationId(), ex.getMessage());
                } else {
                    logger.info("Sent log event for org={} to topic={} partition={} offset={}",
                            logEvent.getOrganizationId(),
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });

        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize log event: {}", logEvent.getId(), e);
            throw new RuntimeException(e);
        }
    }
}
