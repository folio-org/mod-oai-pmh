package org.folio.oaipmh.helpers.referencedata;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Generic implementation of cache.
 * Expected to be thread-safe, and can be safely accessed by multiple concurrent threads.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 */
public class Cache<K, V> {
  private com.github.benmanes.caffeine.cache.Cache<K, V> delegate;

  public Cache(int expirationAfterAccess) {
    this.delegate = Caffeine.newBuilder()
      .executor(Executors.newSingleThreadExecutor())
      .expireAfterAccess(expirationAfterAccess, TimeUnit.SECONDS)
      .build();
  }

  /**
   * Returns the value associated with the {@code key} in this cache, or {@code null} if there is no
   * cached value for the {@code key}.
   *
   * @param key the key whose associated value is to be returned
   * @return the value to which the specified key is mapped, or {@code null} if this map
   *         contains no mapping for the key
   * @throws NullPointerException if the specified key is null
   */
  public V get(K key) {
    return this.delegate.getIfPresent(key);
  }

  /**
   * Associates the {@code value} with the {@code key} in this cache. If the cache previously
   * contained a value associated with the {@code key}, the old value is replaced by the new
   * {@code value}.
   *
   * @param key   the key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @throws NullPointerException if the specified key or value is null
   */
  public void put(K key, V value) {
    this.delegate.put(key, value);
  }
}

