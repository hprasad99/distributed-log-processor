package org.project.logprocessor.service;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class LogGenerationService {

  private static final Logger logger = LoggerFactory.getLogger(LogGenerationService.class);

  private final KafkaProducerService kafkaProducerService;
  private final RateLimitingService rateLimitingService;
  private final ExecutorService executorService;
  private final Random random = new Random();

  private final AtomicLong generatedCounter = new AtomicLong(0);
  private final AtomicLong rateLimitedCounter = new AtomicLong(0);
  private final AtomicInteger currentRatePerSecond = new AtomicInteger(0);

  public LogGenerationService(
      KafkaProducerService kafkaProducerService,
      RateLimitingService rateLimitingService,
      ExecutorService executorService) {
    this.kafkaProducerService = kafkaProducerService;
    this.rateLimitingService = rateLimitingService;
    this.executorService = executorService;
  }
}
