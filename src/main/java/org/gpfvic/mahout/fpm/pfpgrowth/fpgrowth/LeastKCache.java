package org.gpfvic.mahout.fpm.pfpgrowth.fpgrowth;

import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;
import java.util.PriorityQueue;


public class LeastKCache<K extends Comparable<? super K>,V> {
  
  private final int capacity;
  private final Map<K,V> cache;
  private final PriorityQueue<K> queue;
  
  public LeastKCache(int capacity) {
    this.capacity = capacity;
    cache = Maps.newHashMapWithExpectedSize(capacity);
    queue = new PriorityQueue<K>(capacity + 1, Collections.reverseOrder());
  }

  public final V get(K key) {
    return cache.get(key);
  }
  
  public final void set(K key, V value) {
    if (!contains(key)) {
      queue.add(key);
    }
    cache.put(key, value);
    while (queue.size() > capacity) {
      K k = queue.poll();
      cache.remove(k);
    }
  }
  
  public final boolean contains(K key) {
    return cache.containsKey(key);
  }
  
}
