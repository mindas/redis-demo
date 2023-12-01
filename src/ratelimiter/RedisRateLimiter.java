package ratelimiter;

import io.lettuce.core.cluster.RedisClusterClient;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

public final class RedisRateLimiter implements RateLimiter {

    private static final String PREFIX = "/RedisRateLimiter/";

    private final int maxCallsPerMinute;

    @Nonnull
    private final RedisClusterClient redis;

    public RedisRateLimiter(int maxCallsPerMinute, @Nonnull RedisClusterClient redis) {
        this.maxCallsPerMinute = maxCallsPerMinute;
        this.redis = redis;
    }

    @Override
    public boolean exceedingRateLimit(@Nonnull String key) {
        try (var connection = redis.connect()) {
            var commands = connection.sync();
            var fullKey = PREFIX + key;

            var val = commands.get(fullKey);
            int used = 0;
            if (val != null) {
                used = Integer.parseInt(val);
            } else {
                commands.set(fullKey, "0");
                commands.expire(fullKey, TimeUnit.MINUTES.toSeconds(1));
            }
            if (used >= maxCallsPerMinute) {
                return true;
            }

            commands.incr(fullKey);
            commands.expire(fullKey, TimeUnit.MINUTES.toSeconds(1));
            return false;
        }
    }
}
