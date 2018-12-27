package com.bfm.app.timeseries.cache;

@FunctionalInterface
public interface EvictionListener<K, V> {
	
	public void onEvict(K key, V value);

}
