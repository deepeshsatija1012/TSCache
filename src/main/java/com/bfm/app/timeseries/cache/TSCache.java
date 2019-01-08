package com.bfm.app.timeseries.cache;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import com.bfm.app.timeseries.Status;
import com.bfm.app.timeseries.TimeSeriesEntry;
import com.bfm.app.timeseries.TimeSeriesEntryBuilder;
import com.bfm.app.timeseries.TimeSeriesEntryUtils;
import com.bfm.app.timeseries.cache.stats.CacheStats;
import com.bfm.app.timeseries.cache.utils.IdGenerator;
import com.bfm.app.timeseries.classifiers.IntervalType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TSCache<K, V extends TimeSeriesEntry, D> {
	private EvictionStrategy<K, V> evictionStrategy = new EvictionStrategy.NoneEvictionStrategy<>();
	static class Node<K, V> implements Linked<Node<K,V>> {
		private Node<K, V> prev;
		private K key;
		private V value;
		private Node<K, V> next;

		public Node(K key, V value) {
			this.value = value;
			this.key = key;
		}
		
		public K getKey() {
			return this.key;
		}

		public V getValue() {
			return this.value;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			@SuppressWarnings("unchecked")
			Node<K, V> other = (Node<K, V>) obj;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			if (value == null) {
				if (other.value != null)
					return false;
			} else if (!value.equals(other.value))
				return false;
			return true;
		}

		@Override
		public Node<K, V> getPrevious() {
			return prev;
		}

		@Override
		public void setPrevious(Node<K, V> prev) {
			this.prev = prev;
		}

		@Override
		public Node<K, V> getNext() {
			return next;
		}

		@Override
		public void setNext(Node<K, V> next) {
			this.next = next;
		}
	}

	static class LRUNode<K, V> extends Node<K, V> implements Comparable<LRUNode<K, V>> {
		private long timeStamp;
		public LRUNode(K key, V value) {
			super(key, value);
			this.timeStamp = System.currentTimeMillis();
		}

		public V getValue() {
			this.timeStamp = System.currentTimeMillis();
			return super.getValue();
		}

		public long getTimeStamp() {
			return timeStamp;
		}

		@Override
		public int compareTo(LRUNode<K, V> that) {
			return Long.compare(this.timeStamp, that.timeStamp);
		}
	}

	static class LFUNode<K, V> extends Node<K, V> implements Comparable<LFUNode<K, V>> {
		private long timeStamp;
		private AtomicLong count = new AtomicLong(0);
		public LFUNode(K key, V value) {
			super(key, value);
			this.timeStamp = IdGenerator.getNextInSequence();
		}

		public V getValue() {
			this.timeStamp = IdGenerator.getNextInSequence();
			this.count.incrementAndGet();
			return super.getValue();
		}

		public long getCount() {
			return count.get();
		}
		
		public long getTimeStamp() {
			return timeStamp;
		}

		@Override
		public int compareTo(LFUNode<K, V> that) {
			int cmp = Long.compare(this.count.get(), that.count.get());
			if(cmp==0) {
				cmp = Long.compare(this.timeStamp, that.timeStamp);
			}
			return cmp;
		}
	}

	private ConcurrentHashMap<K, Node<K, V>> cache = new ConcurrentHashMap<>();
	private IntervalType interval;
	private DataLoader<K, D> dataLoader;
	private Function<D, V> transformer;
	private CacheStats cacheStats = new CacheStats();
	private final String cacheName;
	private int allowedCacheSize = Integer.MIN_VALUE;
	private Semaphore sizeControlSemaphore;
	
	private EvictionListener<K, V> listener;
	
	private final Map<String, Class<?>> fieldTypeMap = new HashMap<>();
	private long start, end;
	private Supplier<V> objectSupplier;

	public TSCache(String cacheName, IntervalType intervalType, Function<K, D> singleEntryLoader,
			Function<Set<K>, Map<K, D>> multipleEntryLoader, Supplier<Map<K, D>> allEntriesLoader,
			Function<D, V> transformer, int allowedCacheSize, EvictionStrategy<K, V> evictionStrategy, 
			EvictionListener<K, V> listener, LocalDateTime startDate, LocalDateTime endDate, Map<String, Class<?>> fieldTypeMap,
			Supplier<V> objectSupplier) {
		this.cacheName = cacheName;
		this.interval = intervalType.initialize();
		this.transformer = transformer;
		this.dataLoader = new DataLoader<>(singleEntryLoader, multipleEntryLoader, allEntriesLoader, cacheStats);
		if(allowedCacheSize!=Integer.MIN_VALUE) {
			this.allowedCacheSize = allowedCacheSize;
			sizeControlSemaphore = new Semaphore(allowedCacheSize, true);
		}
		if(evictionStrategy!=null)
			this.evictionStrategy = evictionStrategy;
		
		this.listener = listener;
		
		this.fieldTypeMap.putAll(fieldTypeMap);
		this.start = startDate.atZone(ZoneId.systemDefault()).toEpochSecond(); this.end = endDate.atZone(ZoneId.systemDefault()).toEpochSecond();
		this.objectSupplier = objectSupplier;
	}

	private void evict(int permits) {
		if(evictionStrategy!=null) {
			Map<K, V> evicetedEntries = evictionStrategy.evict(permits, cache);
			sizeControlSemaphore.release(evicetedEntries.size());
			for(Map.Entry<K, V> entry : evicetedEntries.entrySet()) {
				listener.onEvict(entry.getKey(), entry.getValue());
			}
		}
	}
	
	private void acquireWithEvict(int permits) {
		while(sizeControlSemaphore!=null && !sizeControlSemaphore.tryAcquire(permits)) {
			evict(permits);
		}
	}
	
	public void loadAll() {
		dataLoader.getAllEntriesLoader().get().entrySet().parallelStream().forEach(entry -> {
			V newValue;
			if (entry.getValue() != null && (newValue = transformer.apply(entry.getValue())) != null) {
				acquireWithEvict(1);
				cache.put(entry.getKey(), evictionStrategy.getNode(entry.getKey(), newValue));
			}
		});
	}
	
	private V getQuietly(K key) {
		Node<K, V> node = cache.get(key);
		if(node!=null) {
			evictionStrategy.applyRead(node);
			return node.getValue();
		}
		return null;
	}

	public V get(K key) {
		V val;
		Node<K, V> node = cache.computeIfAbsent(key, k -> {
			acquireWithEvict(1);
			Optional<V> value = Optional.ofNullable(dataLoader.getSingleEntryLoader().apply(k)).map(transformer);
			return value.isPresent() ? evictionStrategy.getNode(k, value.get()) : null;
		});
		if(node==null) {
			val = null;
		}else {
			val = node.getValue();
			cacheStats.incHitCounter();
		}
		val = node == null?null:node.getValue();
		if(node!=null)
			evictionStrategy.applyRead(node);
		TimeSeriesEntryUtils.updateStatus(val);
		return val;
	}

	public Map<K, V> get(Set<K> keys) {
		int hits = 0;
		Map<K, V> result = Maps.newLinkedHashMap();
		Set<K> keysToLoad = Sets.newLinkedHashSet();
		for (K key : keys) {
			V value = getQuietly(key);
			if (!result.containsKey(key)) {
				result.put(key, value);
				if (value == null) {
					keysToLoad.add(key);
				} else {
					hits++;
				}
			}
		}
		if(keysToLoad.size()>allowedCacheSize) {
			return ImmutableMap.copyOf(result);
		}
		try {
			if (!keysToLoad.isEmpty()) {
				Map<K, D> newEntries = Optional.ofNullable(dataLoader.getMultipleEntryLoader().apply(keysToLoad))
						.orElseGet(Collections::emptyMap);
				for (K key : keysToLoad) {
					D value = newEntries.get(key);
					if (value != null) {
						V newValue = transformer.apply(value);
						acquireWithEvict(1);
						result.put(key, transformer.apply(value));
						cache.putIfAbsent(key, evictionStrategy.getNode(key, newValue));
						TimeSeriesEntryUtils.updateStatus(newValue);
						hits++;
					}
				}
			}
			return ImmutableMap.copyOf(result);
		} finally {
			cacheStats.updateHitCounter(hits);
		}
	}
	
	private ThreadLocal<TimeSeriesEntryBuilder<V>> threadlocalBuilders = new ThreadLocal<TimeSeriesEntryBuilder<V>>() {
		public TimeSeriesEntryBuilder<V> get() {
			return super.get().reset();
		};
		
		protected TimeSeriesEntryBuilder<V> initialValue() {
			return new TimeSeriesEntryBuilder<>();
		};
	};
	
	private Node<K,V> getTimeSeriesValue(K key, Node<K, V> node) {
		V value = node.getValue();
		if(value.getStoredStartTime()<this.start || value.getStoredEndTime()>this.end) {
			return node;
		}
		value = TimeSeriesEntryUtils.restrictedClone(value, start, end, interval, threadlocalBuilders.get(), fieldTypeMap, 
				objectSupplier);
		return evictionStrategy.getNode(node.getKey(), value);
	}
	
	public V get(K key, long start, long end) {
		V val;
		Node<K, V> node = cache.computeIfAbsent(key, k -> {
			acquireWithEvict(1);
			Optional<V> value = Optional.ofNullable(dataLoader.getSingleEntryLoader().apply(k)).map(transformer);
			return value.isPresent() ? evictionStrategy.getNode(k, value.get()) : null;
		});
		
		if(node==null) {
			val = null;
		}else {
			if(TimeSeriesEntryUtils.canUseThis(node.getValue(), start, end)) {
				if(TimeSeriesEntryUtils.getStatus(node.getValue())==Status.JUST_LOADED) {
					cache.computeIfPresent(key, (k, v) -> {
						if (TimeSeriesEntryUtils.updateStatus(node.getValue()))
							return getTimeSeriesValue(key, node);
						else
							return v;
					});
				}
				val = node.getValue();
			}else {
				Optional<V> value = Optional.ofNullable(dataLoader.getSingleEntryLoader().apply(key)).map(transformer);
				val = value.isPresent() ? value.get() : null;
			}
			cacheStats.incHitCounter();
		}
		val = node == null?null:node.getValue();
		if(node!=null)
			evictionStrategy.applyRead(node);
		TimeSeriesEntryUtils.updateStatus(val);
		return val;
	}
	
	public Map<K, V> get(Set<K> keys, long start, long end) {
		int hits = 0;
		Map<K, V> result = Maps.newLinkedHashMap();
		Set<K> keysToLoad = Sets.newLinkedHashSet();
		for (K key : keys) {
			V value = getQuietly(key);
			if (!result.containsKey(key)) {
				result.put(key, value);
				if (value == null || TimeSeriesEntryUtils.canUseThis(value, start, end)) {
					keysToLoad.add(key);
				} else {
					hits++;
				}
			}
		}
		if(keysToLoad.size()>allowedCacheSize) {
			return ImmutableMap.copyOf(result);
		}
		try {
			if (!keysToLoad.isEmpty()) {
				Map<K, D> newEntries = Optional.ofNullable(dataLoader.getMultipleEntryLoader().apply(keysToLoad))
						.orElseGet(Collections::emptyMap);
				for (K key : keysToLoad) {
					D value = newEntries.get(key);
					if (value != null) {
						V newValue = transformer.apply(value);
						acquireWithEvict(1);
						result.put(key, newValue);
						Node<K, V> node = evictionStrategy.getNode(key, newValue);
						if(cache.putIfAbsent(key, node)==null) {
							if(TimeSeriesEntryUtils.getStatus(node.getValue())==Status.JUST_LOADED) {
								cache.computeIfPresent(key, (k, v) -> {
									if (TimeSeriesEntryUtils.updateStatus(node.getValue()))
										return getTimeSeriesValue(key, node);
									else
										return v;
								});
							}
						}
						TimeSeriesEntryUtils.updateStatus(newValue);
						hits++;
					}
				}
			}
			return ImmutableMap.copyOf(result);
		} finally {
			cacheStats.updateHitCounter(hits);
		}
	}

	public String getCacheName() {
		return cacheName;
	}
	
	public void clear() {
		this.cache.clear();
	}

}
