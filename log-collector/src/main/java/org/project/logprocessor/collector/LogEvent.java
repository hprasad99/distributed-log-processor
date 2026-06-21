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
