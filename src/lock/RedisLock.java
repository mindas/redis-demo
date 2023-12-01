package lock;

import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.RedisClusterClient;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

public final class RedisLock implements Lock {

    private static final String PREFIX = "/RedisLock/";

    @Nonnull
    private final String name;

    private final RedisClusterClient redis;

    public RedisLock(@Nonnull String name, RedisClusterClient redis) {
        this.name = name;
        this.redis = redis;
    }

    @Override
    public boolean tryLock() {
        try (var connection = redis.connect()) {
            var result = connection.sync().set(
                    PREFIX + name,
                    "1",
                    new SetArgs().nx().ex(Duration.of(1, ChronoUnit.MINUTES))
            );
            return "OK".equals(result);
        }
    }

    @Override
    public void lock() {
        while (!tryLock()) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
    }

    @Override
    public void unlock() {
        try (var connection = redis.connect()) {
            connection.sync().del(PREFIX + name);
        }
    }

    //////////////////////////////// not implemented below ////////////////////////////////

    @Override
    public void lockInterruptibly() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
