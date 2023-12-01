package cache;

import com.google.common.cache.CacheBuilder;

import javax.annotation.Nonnull;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public final class GuavaBackedCache implements Cache<String, String>, OurLoadingCache<String, String> {

    private final com.google.common.cache.Cache<String, String> cache = CacheBuilder.newBuilder().build();

    @Override
    public String get(@Nonnull String key) {
        return cache.getIfPresent(key);
    }

    @Override
    public String get(@Nonnull String key, @Nonnull Function<String, String> loadingFunction) {
        try {
            return cache.get(key, () -> loadingFunction.apply(key));
        } catch (NoSuchElementException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(String key, String value) {
        cache.put(key, value);
    }

    @Override
    public boolean remove(String key) {
        var prevVal = get(key);
        cache.invalidate(key);
        return prevVal != null;
    }

    //////////////////////////////////////// unsupported stuff below //////////////////////////////////

    @Override
    public Map<String, String> getAll(Set<? extends String> set) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(String k) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadAll(Set<? extends String> set, boolean b, CompletionListener completionListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getAndPut(String k, String v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean putIfAbsent(String k, String v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(String k, String v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getAndRemove(String k) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(String k, String v, String v1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(String k, String v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getAndReplace(String k, String v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(Set<? extends String> set) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <C extends Configuration<String, String>> C getConfiguration(Class<C> aClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invoke(String k, EntryProcessor<String, String, T> entryProcessor, Object... objects) throws EntryProcessorException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<String, EntryProcessorResult<T>> invokeAll(Set<? extends String> set, EntryProcessor<String, String, T> entryProcessor, Object... objects) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CacheManager getCacheManager() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> aClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<String, String> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<String, String> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        throw new UnsupportedOperationException();
    }
}
