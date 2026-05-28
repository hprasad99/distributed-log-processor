package org.project.logprocessor.model;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.MapKeyColumn;
import javax.persistence.Table;

import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "log_events")
@Getter
@Setter
public class LogEvent {

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private UUID id;

  @Column(name = "correlation_id", nullable = false)
  private String correlationId;

  @Column(name = "timestamp", nullable = false)
  private Instant timestamp;

  @Column(name = "level", nullable = false)
  private String level;

  @Column(name = "source", nullable = false)
  private String source;

  @Column(name = "message", nullable = false, length = 1000)
  private String message;

  @ElementCollection
  @CollectionTable(name = "log_event_metadata", joinColumns = @JoinColumn(name = "log_event_id"))
  @MapKeyColumn(name = "key")
  @Column(name = "value")
  private Map<String, String> metadata;

  @Column(name = "trace_id")
  private String traceId;

  public LogEvent() {
    this.id = UUID.randomUUID();
    this.correlationId = UUID.randomUUID().toString();
    this.timestamp = Instant.now();
  }

  public LogEvent(String level, String source, String message, Map<String, String> metadata) {
    this();
    this.level = level;
    this.source = source;
    this.message = message;
    this.metadata = metadata;
  }
}
