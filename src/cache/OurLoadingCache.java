package cache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

public interface OurLoadingCache<K, V> {
    @Nullable
    V get(@Nonnull K key, @Nonnull Function<K, V> loadingFunction);
}
