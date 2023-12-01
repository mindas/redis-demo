package ratelimiter;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface RateLimiter {

    boolean exceedingRateLimit(@Nonnull String key);
}
