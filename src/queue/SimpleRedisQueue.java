package queue;

import io.lettuce.core.cluster.RedisClusterClient;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

public final class SimpleRedisQueue implements Queue<String> {

    private static final String PREFIX = "/RedisQueue/";

    @Nonnull
    private final RedisClusterClient redis;

    @Nonnull
    private final String queueName;

    public SimpleRedisQueue(@Nonnull RedisClusterClient redis, @Nonnull String queueName) {
        this.redis = redis;
        this.queueName = queueName;
    }

    @Override
    public boolean add(String s) {
        try (var connection = redis.connect()) {
            connection.sync().rpush(PREFIX + queueName, s);
        }
        return true;
    }

    @Override
    public boolean offer(String s) {
        return add(s);
    }

    @Override
    public String remove() {
        var result = poll();
        if (result == null) {
            throw new NoSuchElementException();
        } else {
            return result;
        }
    }

    @Override
    public String poll() {
        try (var connection = redis.connect()) {
            return connection.sync().lpop(PREFIX + queueName);
        }
    }

    @Override
    public String element() {
        var result = peek();
        if (result == null) {
            throw new NoSuchElementException();
        } else {
            return result;
        }
    }

    @Override
    public String peek() {
        try (var connection = redis.connect()) {
            var result = connection.sync().lrange(PREFIX + queueName, 0, 0);
            if (!result.isEmpty()) {
                return result.get(0);
            } else {
                return null;
            }
        }
    }

    /////////////////////////// nothing below is implemented ///////////////////////////

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
