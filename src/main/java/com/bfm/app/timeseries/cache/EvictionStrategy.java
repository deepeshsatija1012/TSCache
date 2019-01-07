package com.bfm.app.timeseries.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
//import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;

import com.bfm.app.timeseries.cache.TSCache.LFUNode;
import com.bfm.app.timeseries.cache.TSCache.LRUNode;
import com.bfm.app.timeseries.cache.TSCache.Node;

public interface EvictionStrategy<K, V> {
	public void applyRead(Node<K, V> node);
	public void applyWrite(Node<K, V> node);
	public Node<K, V> getNode(K key, V value);
	public Map<K, V> evict(int permits, ConcurrentHashMap<K, Node<K, V>> cache);
	public String queue(); 
	
	public static class NoneEvictionStrategy<K, V> implements EvictionStrategy<K, V>{

		@Override
		public void applyRead(Node<K, V> node) {
		}

		@Override
		public void applyWrite(Node<K, V> node) {
		}

		@Override
		public Node<K, V> getNode(K key, V value) {
			return new Node<K, V>(key, value);
		}

		@Override
		public Map<K, V> evict(int permits, ConcurrentHashMap<K, Node<K, V>> cache) {
			return Collections.emptyMap();
		}

		@Override
		public String queue() {
			return StringUtils.EMPTY;
		}
		
	}
	
	public static class LRUEvictionStrategy<K, V> implements EvictionStrategy<K, V>{
		private final LinkedDequeThreadSafe<Node<K, V>> evictionQueue = new LinkedDequeThreadSafe<>();
		
		@Override
		public void applyRead(Node<K, V> node) {
			if(evictionQueue.contains(node))
				evictionQueue.moveToBack(node);
		}

		@Override
		public void applyWrite(Node<K, V> node) {
			evictionQueue.add(node);
			
		}

		@Override
		public Node<K, V> getNode(K key, V value) {
			Node<K, V> node = new LRUNode<>(key, value);
			applyWrite(node);
			return node;
		}

		@Override
		public Map<K, V> evict(int permits, ConcurrentHashMap<K, Node<K, V>> cache) {
			Map<K, V> evictedEntries = new HashMap<>();
			for(int i=0;i<permits;i++) {
				Node<K, V> evicted = evictionQueue.pollFirst();
				cache.remove(evicted.getKey());
				evictedEntries.put(evicted.getKey(), evicted.getValue());
			}
			return evictedEntries;
		}

		@Override
		public String queue() {
			Spliterator<Node<K, V>> spliterator = Spliterators.spliteratorUnknownSize(evictionQueue.iterator(), 0); 
			return StreamSupport.stream(spliterator, false).map(n -> n.getKey().toString()).collect(Collectors.joining("->"));
		}
		
	}
	
	public static class LFUEvictionStrategy<K, V> implements EvictionStrategy<K, V>{
		private final PriorityBlockingQueue<Node<K, V>> evictionQueue = new PriorityBlockingQueue<>();
		
		@Override
		public void applyRead(Node<K, V> node) {
			evictionQueue.add(evictionQueue.poll());
		}

		@Override
		public void applyWrite(Node<K, V> node) {
			evictionQueue.add(node);
		}

		@Override
		public Node<K, V> getNode(K key, V value) {
			Node<K, V> node = new LFUNode<>(key, value);
			applyWrite(node);
			return node;
		}

		@Override
		public Map<K, V> evict(int permits, ConcurrentHashMap<K, Node<K, V>> cache) {
			Map<K, V> evictedEntries = new HashMap<>();
			for(int i=0;i<permits;i++) {
				Node<K, V> evicted = evictionQueue.poll();
				cache.remove(evicted.getKey());
				evictedEntries.put(evicted.getKey(), evicted.getValue());
			}
			return evictedEntries;
		}

		@Override
		public String queue() {
			Spliterator<Node<K, V>> spliterator = Spliterators.spliteratorUnknownSize(evictionQueue.iterator(), 0); 
			return StreamSupport.stream(spliterator, false).map(n -> n.getKey().toString()+"["+((LFUNode<K, V>)n).getCount()+"]["+((LFUNode<K, V>)n).getTimeStamp()+"]").collect(Collectors.joining("->"));
		}
		
	}

}
