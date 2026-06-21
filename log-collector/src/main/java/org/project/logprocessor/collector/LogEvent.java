package org.project.logprocessor.collector;

import java.time.LocalDateTime;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LogEvent {
  private String id;
  private String filePath;
  private String content;
  private long fileOffset;
  private LocalDateTime timestamp;
  private String hostname;
  private String serviceName;
  private String contentHash;

  public LogEvent(String filePath, String content, long fileOffset) {
    this.filePath = filePath;
    this.content = content;
    this.fileOffset = fileOffset;
    this.timestamp = LocalDateTime.now();
    this.hostname = System.getenv().getOrDefault("HOSTNAME", "localhost");
    this.serviceName = "log-collector";
    this.id = generateId();
  }

  private String generateId() {
    return String.format("%s-%d-%s", filePath.hashCode(), fileOffset, timestamp.toString());
  }

  @Override
  public String toString() {
    return String.format(
        "LogEvent{id='%s', filePath='%s', content='%s', offset=%d}",
        id, filePath, content.substring(0, Math.min(50, content.length())), fileOffset);
  }
}
