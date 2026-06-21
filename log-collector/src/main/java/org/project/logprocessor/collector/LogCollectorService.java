package org.project.logprocessor.collector;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class LogCollectorService {
  private static final Logger logger = LoggerFactory.getLogger(LogCollectorService.class);

  @Value("#{'${log-collector.watch-directories:/tmp/logs}'.split(',')}")
  private List<String> watchDirectories;

  private String filePatterns;

  @Autowired private OffsetManager offsetManager;

  @Autowired private KafkaProducerService kafkaProducerService;

  private WatchService watchService;
  private final ExecutorService watcherPool = Executors.newFixedThreadPool(4);
  private final ExecutorService processingPool = Executors.newFixedThreadPool(8);
  private final ConcurrentHashMap<Path, WatchKey> watchedPaths = new ConcurrentHashMap<>();
  private final AtomicLong processedEvents = new AtomicLong(0);
  private final AtomicLong skippedDuplicates = new AtomicLong(0);

  @PostConstruct
  public void initialize() {
    try {
      watchService = FileSystems.getDefault().newWatchService();
      startWatching();
      logger.info("Log collector service initialized successfully");
    } catch (IOException ex) {
      logger.error("Failed to initialize log collector service", ex);
      throw new RuntimeException("Failed to initialize file watching", ex);
    }
  }

  private void startWatching() {
    for (String directory : watchDirectories) {
      Path path = Paths.get(directory.trim());
      registerDirectory(path);
    }

    // start the watching thread
    watcherPool.submit(this::watchForFileChanges);

    // perform initial scan
    watcherPool.submit(this::performInitialScan);
  }

  private void performInitialScan() {
    logger.info("Performing initial scan of watch directories...");
    for (String directory : watchDirectories) {
      Path dirPath = Paths.get(directory.trim());
      if (Files.exists(dirPath)) {
        scanDirectory(dirPath);
      }
    }
  }

  private void scanDirectory(Path dirPath) {
    try {
      Files.walkFileTree(
          dirPath,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              if (isLogFile(file)) {
                processingPool.submit(() -> processLogFile(file));
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException ex) {
      logger.error("Error scanning directory: " + dirPath, ex);
    }
  }

  @Async
  private void watchForFileChanges() {
    logger.info("Starting file change monitoring...");
    while (!Thread.currentThread().isInterrupted()) {
      try {
        WatchKey key = watchService.poll(100, TimeUnit.MILLISECONDS);
        if (key != null) {
          processWatchEvents(key);
          key.reset();
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception ex) {
        logger.error("Error in file watching", ex);
      }
    }
  }

  private void processWatchEvents(WatchKey key) {
    for (WatchEvent<?> event : key.pollEvents()) {
      WatchEvent.Kind<?> kind = event.kind();
      if (kind == StandardWatchEventKinds.OVERFLOW) {
        logger.warn("Watch event overflow detected");
        continue;
      }

      Path path = (Path) event.context();
      Path fullPath = ((Path) key.watchable()).resolve(path);

      if (isLogFile(fullPath)) {
        logger.debug("File change detected: {} ({})", fullPath, kind.name());
        processingPool.submit(() -> processLogFile(fullPath));
      }
    }
  }

  private void processLogFile(Path filePath) {
    try {
      long currentOffset = offsetManager.getOffset(filePath.toString());
      long fileSize = Files.size(filePath);

      if (currentOffset >= fileSize) {
        return;
      }
      List<String> newLines =
          Files.readAllLines(filePath).stream()
              .skip(currentOffset == 0 ? 0 : countLines(filePath, currentOffset))
              .toList();

      for (String line : newLines) {
        if (!line.trim().isEmpty()) {
          processLogEntry(filePath.toString(), line, currentOffset);
          currentOffset += line.length() + System.lineSeparator().length();
        }
      }

      offsetManager.commitOffset(filePath.toString(), fileSize);
    } catch (Exception ex) {
      logger.error("Error processing log file: " + filePath, ex);
    }
  }

  private void processLogEntry(String filePath, String content, long offset) {
    try {
      LogEvent logEvent = new LogEvent(filePath, content, offset);

      // check for duplicates
      if (offsetManager.isDuplicate(logEvent.getContentHash())) {
        skippedDuplicates.incrementAndGet();
        logger.debug("Skipped duplicate log entry: {}", logEvent.getId());
        return;
      }

      kafkaProducerService.sendLogEvent(logEvent);
      processedEvents.incrementAndGet();
    } catch (Exception ex) {
      logger.error("Error processing log entry", ex);
    }
  }

  private long countLines(Path filePath, long offset) throws IOException {
    if (offset == 0) return 0;
    try (Stream<String> lines = Files.lines(filePath)) {
      return lines.limit(offset).count();
    }
  }

  private boolean isLogFile(Path fullPath) {
    String fileName = fullPath.getFileName().toString();
    return fileName.endsWith(".log") && Files.isRegularFile(fullPath);
  }

  private void registerDirectory(Path directory) {
    try {
      if (!Files.exists(directory)) {
        Files.createDirectories(directory);
        logger.info("Created watch directory: {}", directory);
      }

      WatchKey key =
          directory.register(
              watchService,
              StandardWatchEventKinds.ENTRY_MODIFY,
              StandardWatchEventKinds.ENTRY_CREATE);
      watchedPaths.put(directory, key);
      logger.info("Registered directory for watching: {}", directory);
    } catch (IOException ex) {
      logger.error("Failed to register directory for watching: " + directory, ex);
    }
  }

  @Scheduled(fixedRate = 60000) // every minute
  public void logStatistics() {
    logger.info(
        "Collector stats - Processed: {}, Duplicates skipped: {}, Kafka sent: {}, Kafka failed: {}",
        processedEvents.get(),
        skippedDuplicates.get(),
        kafkaProducerService.getSentEventsCount(),
        kafkaProducerService.getFailedEventsCount());
  }

  @PreDestroy
  public void shutdown() {
    logger.info("Shutting down log collector service...");
    watcherPool.shutdown();
    processingPool.shutdown();

    try {
      if (!watcherPool.awaitTermination(10, TimeUnit.SECONDS)) {
        watcherPool.shutdownNow();
      }
      if (!processingPool.awaitTermination(10, TimeUnit.SECONDS)) {
        processingPool.shutdownNow();
      }
      if (watchService != null) {
        watchService.close();
      }
    } catch (Exception ex) {
      logger.error("Error during shutdown", ex);
    }
  }

  public long getProcessedEventsCount() {
    return processedEvents.get();
  }

  public long getSkippedDuplicatesCount() {
    return skippedDuplicates.get();
  }
}
