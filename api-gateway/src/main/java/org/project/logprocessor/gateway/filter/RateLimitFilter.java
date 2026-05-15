package org.project.logprocessor.gateway.filter;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

@Component
public class RateLimitFilter extends AbstractGatewayFilterFactory<RateLimitFilter.Config> {

  @Autowired private ReactiveRedisTemplate<String, String> redisTemplate;

  public RateLimitFilter() {
    super(Config.class);
  }

  @Override
  public GatewayFilter apply(RateLimitFilter.Config config) {
    return (exchange, chain) -> {
      String clientId = getClientId(exchange);
      String key = "rate_limit" + clientId;

      ReactiveValueOperations<String, String> ops = redisTemplate.opsForValue();
      return ops.get(key)
          .cast(String.class)
          .defaultIfEmpty("0")
          .flatMap(
              currentCount -> {
                int count = Integer.parseInt(currentCount);
                if (count >= config.getLimit()) {
                  exchange.getResponse().setComplete();
                }

                return ops.increment(key)
                    .flatMap(
                        newCount -> {
                          if (newCount == 1) {
                            return redisTemplate
                                .expire(key, Duration.ofSeconds(config.getWindow()))
                                .then(chain.filter(exchange));
                          }
                          return chain.filter(exchange);
                        });
              });
    };
  }

  private String getClientId(ServerWebExchange exchange) {
    return exchange.getRequest().getRemoteAddress().getAddress().getHostAddress();
  }

  public static class Config {
    private int limit = 100;
    private int window = 60;

    public int getLimit() {
      return limit;
    }

    public void setLimit(int limit) {
      this.limit = limit;
    }

    public int getWindow() {
      return window;
    }

    public void setWindow(int window) {
      this.window = window;
    }
  }
}
