package org.project.logprocessor.service;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.RedisTemplate;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class RateLimitingService {
  private final RedisTemplate<String, String> redisTemplate;
  private final Timer rateLimitTimer;

  public RateLimitingService(
      RedisTemplate<String, String> redisTemplate, MeterRegistry meterRegistry) {
    this.redisTemplate = redisTemplate;
    this.rateLimitTimer =
        Timer.builder("rate_limit_check")
            .description("Time taken to check rate limits")
            .register(meterRegistry);
  }

  /**
   * Sliding window rate limiting using Redis sorted sets
   *
   * @param key Rate limit key (e.g., "generator:instance1")
   * @param windowSizeSeconds Size of the rate limiting window
   * @param maxRequests Maximum requests allowed in the window
   * @return true if request is allowed, false if rate limited
   */
  public boolean isAllowed(String key, long windowSizeSeconds, long maxRequests) {
    try {
      return rateLimitTimer.recordCallable(
          () -> {
            try {
              long now = Instant.now().getEpochSecond();
              long windowStart = now - windowSizeSeconds;

              // remove expired entries
              redisTemplate.opsForZSet().removeRangeByScore(key, 0, windowStart);

              // count current entries
              Long currentCount = redisTemplate.opsForZSet().count(key, windowStart, now);
              if (currentCount < maxRequests) {
                // add current request
                redisTemplate.opsForZSet().add(key, String.valueOf(now), now);
                redisTemplate.expire(key, windowSizeSeconds + 1, TimeUnit.SECONDS);
                return true;
              }
              return false;
            } catch (Exception e) {
              return true;
            }
          });
    } catch (Exception ex) {
      return true;
    }
  }

  /** Get current rate for a key */
  public long getCurrentRate(String key, long windowSizeSeconds) {
    try {
      long now = Instant.now().getEpochSecond();
      long windowStart = now - windowSizeSeconds;
      return redisTemplate.opsForZSet().count(key, windowStart, now);
    } catch (Exception ex) {
      return 0L;
    }
  }
}
