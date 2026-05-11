package org.project.logprocessor.consumer.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "log_events")
@Getter
@Setter
public class LogEvent {

  @Id private String id;

  @Column(name = "organization_id", nullable = false)
  private String organizationId;

  @Column(name = "level", nullable = false)
  private String level;

  @Column(name = "message", columnDefinition = "TEXT")
  private String message;

  @Column(name = "source")
  private String source;

  @Column(name = "timestamp", nullable = false)
  private LocalDateTime timestamp;

  @Column(name = "processed_at")
  private LocalDateTime processadAt;

  public LogEvent() {}
}
