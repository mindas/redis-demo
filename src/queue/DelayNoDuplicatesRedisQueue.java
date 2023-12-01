package queue;

import io.lettuce.core.ZAddArgs;
import io.lettuce.core.cluster.RedisClusterClient;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

public final class DelayNoDuplicatesRedisQueue implements Queue<String> {

    private static final String PREFIX = "/DelayNoDuplicatesRedisQueue/";

    @Nonnull
    private final RedisClusterClient redis;

    @Nonnull
    private final String queueName;

    private final long delay;

    public DelayNoDuplicatesRedisQueue(@Nonnull RedisClusterClient redis, @Nonnull String queueName, long delayMillis) {
        this.redis = redis;
        this.queueName = queueName;
        this.delay = delayMillis;
    }

    @Override
    public boolean offer(String s) {
        try (var connection = redis.connect()) {
            return connection.sync().zadd(
                    PREFIX + queueName,
                    new ZAddArgs().nx(),
                    (double) (System.currentTimeMillis() + delay),
                    s
            ) > 0;
        }
    }

    @Override
    public String poll() {
        try (var connection = redis.connect()) {
            var head = connection.sync().zpopmin(PREFIX + queueName, 1L);       // atomic removal!
            if (head.size() == 1) {
                var value = head.get(0);
                if (value.getScore() > (double) System.currentTimeMillis()) {
                    connection.sync().zadd(PREFIX + queueName, value.getScore(), value.getValue());
                    return null;
                } else {
                    return value.getValue();
                }
            } else {
                assert head.isEmpty();
                return null;
            }
        }
    }

    /////////////////////////// nothing below is implemented ///////////////////////////

    @Override
    public String remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(String s) {
        throw new UnsupportedOperationException();
    }


    @Override
    public String element() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String peek() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<String> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
