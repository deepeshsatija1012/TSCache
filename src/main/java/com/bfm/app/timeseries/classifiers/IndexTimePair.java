package com.bfm.app.timeseries.classifiers;

import java.util.Map;

public class IndexTimePair {
	
	Map.Entry<Long, Integer> pair;
	
	private IndexTimePair(Map.Entry<Long, Integer> pair) {
		this.pair = pair;
	}
	
	public static IndexTimePair of(Map.Entry<Long, Integer> pair) {
		return new IndexTimePair(pair);
	}
	
	public int index() { return pair.getValue(); }
	public long timeSinzeEpoch() { return pair.getKey() ; }

}
