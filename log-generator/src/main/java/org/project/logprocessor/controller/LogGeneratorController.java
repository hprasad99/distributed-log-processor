package org.project.logprocessor.controller;

import java.util.Map;

import org.project.logprocessor.service.LogGenerationService;
import org.project.logprocessor.service.RateLimitingService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/generator")
public class LogGeneratorController {
  private final LogGenerationService logGenerationService;
  private final RateLimitingService rateLimitingService;

  public LogGeneratorController(
      LogGenerationService logGenerationService, RateLimitingService rateLimitingService) {
    this.logGenerationService = logGenerationService;
    this.rateLimitingService = rateLimitingService;
  }

  @PostMapping("/start")
  public ResponseEntity<Map<String, String>> startGeneration() {
    // logGenerationService.start
    return null;
  }
}
