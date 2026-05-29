package org.project.logprocessor.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class LogGenerationService {

  private static final Logger logger = LoggerFactory.getLogger(LogGenerationService.class);

  private final KafkaProducerService kafkaProducerService;

  public LogGenerationService(KafkaProducerService kafkaProducerService) {
    this.kafkaProducerService = kafkaProducerService;
  }
}
