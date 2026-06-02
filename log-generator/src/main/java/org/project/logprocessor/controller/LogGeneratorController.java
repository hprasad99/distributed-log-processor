package org.project.logprocessor.controller;

import org.project.logprocessor.service.LogGenerationService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/generator")
public class LogGeneratorController {
  private final LogGenerationService logGenerationService;

  public LogGeneratorController(LogGenerationService logGenerationService) {
    this.logGenerationService = logGenerationService;
  }
}
