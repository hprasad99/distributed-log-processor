package org.project.logprocessor.producer.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Getter;
import lombok.Setter;
import org.project.logprocessor.producer.enums.LogLevel;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
public class LogEvent {
    private String id;
    private String organizationId;
    private LogLevel level; // INFO, WARN, ERROR
    private String source;
    private String message;
    @JsonFormat(pattern="yyyy-mm-dd'T'HH:mm:ss")
    private LocalDateTime timestamp;

    public LogEvent() {
        this.timestamp = LocalDateTime.now();
        this.id = UUID.randomUUID().toString();
    }

    public LogEvent(String organizationId, String level, String message, String source) {
        this();
        this.organizationId = organizationId;
        this.level = LogLevel.valueOf(level);
        this.message = message;
        this.source = source;
    }
}
