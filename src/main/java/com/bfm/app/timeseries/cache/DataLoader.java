package com.bfm.app.timeseries.cache;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import com.bfm.app.timeseries.cache.stats.CacheStats;

public class DataLoader<K, V> {
	
	private Function<K, V> singleEntryLoader = key -> {return null;};
	private Function<Set<K>, Map<K, V>> multipleEntryLoader =  key -> {return null;};
	private Supplier<Map<K, V>> allEntriesLoader = () -> {return null; };
	public DataLoader(Function<K, V> singleEntryLoader, Function<Set<K>, Map<K, V>> multipleEntryLoader,
			Supplier<Map<K, V>> allEntriesLoader, final CacheStats cacheStats) {
		if(singleEntryLoader!=null) {
			this.singleEntryLoader = key -> {
				long start = System.currentTimeMillis();
				V value = singleEntryLoader.apply(key);
				cacheStats.incMissCounter();
				cacheStats.incLoadCounter(System.currentTimeMillis()-start);
				return value;
			};
		}
		if(multipleEntryLoader!=null) {
			this.multipleEntryLoader = keys -> {
				long start = System.currentTimeMillis();
				Map<K, V> value = multipleEntryLoader.apply(keys);
				cacheStats.updateMissCounter(keys.size());
				cacheStats.incLoadCounter(System.currentTimeMillis()-start);
				return value;
			};
		}
		if(allEntriesLoader!=null) {
			this.allEntriesLoader = () -> {
				long start = System.currentTimeMillis();
				Map<K, V> value = allEntriesLoader.get();
				cacheStats.incLoadCounter(System.currentTimeMillis()-start);
				return value;
			};
		}
	}
	public Function<K, V> getSingleEntryLoader() {
		return singleEntryLoader;
	}
	public Function<Set<K>, Map<K, V>> getMultipleEntryLoader() {
		return multipleEntryLoader;
	}
	public Supplier<Map<K, V>> getAllEntriesLoader() {
		return allEntriesLoader;
	}
	
	
	

}
