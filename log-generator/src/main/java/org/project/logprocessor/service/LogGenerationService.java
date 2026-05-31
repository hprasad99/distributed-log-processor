package org.project.logprocessor.service;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.project.logprocessor.model.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

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
      MeterRegistry meterRegistry) {
    this.kafkaProducerService = kafkaProducerService;
    this.rateLimitingService = rateLimitingService;
    this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    Counter.builder("log_events_generated")
        .description("Total number of log events generated")
        .register(meterRegistry);
    Counter.builder("log_events_rate_limited")
        .description("Number of log events dropped due to rate limited")
        .register(meterRegistry);
  }

  @Value("${app.generator.threads:4}")
  private int threadCount;

  @Value("${app.generator.rate-per-seconds:1000}")
  private int targetRatePerSecond;

  @Value("${app.generator.batch-size:10}")
  private int batchSize;

  @Value("${app.generator.instance-id:generator-1}")
  private String instanceId;

  private volatile boolean isRunning = false;

  // Sample log messages for realistic data generation
  private final List<String> sampleMessages =
      Arrays.asList(
          "User authentication successful",
          "Database connection timeout",
          "Cache miss for key: user_profile",
          "HTTP request processed successfully",
          "Memoory usage above threshold",
          "Batch job completed",
          "Invalid request parameter detected",
          "Service health check passed",
          "Rate limit exceeded for client",
          "Transaction committed successfully");

  private final List<String> logLevels = Arrays.asList("INFO", "WARN", "ERROR", "DEBUG");
  private final List<String> sources =
      Arrays.asList("auth-service", "api-gateway", "user-service", "payment-service");

  public void startGeneration() {
    if (isRunning) {
      logger.warn("Log generation is already running");
      return;
    }

    isRunning = true;
    logger.info(
        "Starting log generation with {} threads, target rate: {} events/sec",
        threadCount,
        targetRatePerSecond);
    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executorService.submit(() -> generationWorker(threadId));
    }
  }

  public void stopGeneration() {
    isRunning = false;
    logger.info("Stopping log generation");
  }

  private void generationWorker(int threadId) {
    String rateLimitKey = String.format("generator:%s:thread:%d", instanceId, threadId);
    int eventsPerThread = targetRatePerSecond / threadCount;
    while (isRunning) {
      try {
        // check rate limiting
        if (!rateLimitingService.isAllowed(rateLimitKey, 1, eventsPerThread)) {
          rateLimitedCounter.incrementAndGet();
          Thread.sleep(100); // back off when rate limited
          continue;
        }
        List<LogEvent> batch = generateLogEventBatch(batchSize);
        kafkaProducerService
            .sendLogEventsBatch(batch)
            .exceptionally(
                throwable -> {
                  logger.error(
                      "Failed to send batch from thread {}: {}", threadId, throwable.getMessage());
                  return null;
                });

        generatedCounter.addAndGet(batch.size());
        long sleepTime = calculateSleepTime(eventsPerThread, batchSize);
        if (sleepTime > 0) {
          Thread.sleep(sleepTime);
        }
      } catch (Exception e) {
        logger.error("Error in generation worker thread {}: {}", threadId, e.getMessage());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  private long calculateSleepTime(int eventsPerSecond, int batchSize) {
    if (eventsPerSecond <= 0) return 1000;
    long batchesPerSecond = eventsPerSecond / batchSize;
    if (batchesPerSecond <= 0) return 1000;
    return 1000 / batchesPerSecond;
  }

  private List<LogEvent> generateLogEventBatch(int batchSize) {
    List<LogEvent> batch = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      LogEvent event = generateSingleLogEvent();
      batch.add(event);
    }
    return batch;
  }

  private LogEvent generateSingleLogEvent() {
    String level = logLevels.get(random.nextInt(logLevels.size()));
    String source = sources.get(random.nextInt(sources.size()));
    String message = sampleMessages.get(random.nextInt(sampleMessages.size()));
    Map<String, String> metadata = new HashMap<>();
    metadata.put("thread_id", String.valueOf(Thread.currentThread().getId()));
    metadata.put("instance_id", instanceId);
    metadata.put("request_id", UUID.randomUUID().toString());
    metadata.put("user_id", "user_" + random.nextInt(10000));
    return new LogEvent(level, source, message, metadata);
  }
}
