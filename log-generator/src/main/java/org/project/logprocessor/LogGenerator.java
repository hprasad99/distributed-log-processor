package org.project.logprocessor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class LogGenerator {
  public static void main(String[] args) {
    SpringApplication.run(LogGenerator.class, args);
  }
}
