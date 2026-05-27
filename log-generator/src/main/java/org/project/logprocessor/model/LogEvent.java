package org.project.logprocessor.model;

import javax.persistence.*;
import java.util.UUID;

@Entity
@Table(name = "log_events")
public class LogEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private UUID id;

    //todo::
}
