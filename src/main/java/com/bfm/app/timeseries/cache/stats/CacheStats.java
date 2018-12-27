package com.bfm.app.timeseries.cache.stats;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class CacheStats {
	
	private AtomicLong hitCounter = new AtomicLong(0);
	private AtomicLong missCounter = new AtomicLong(0);
	private AtomicLong totalLoadCounter = new AtomicLong();
	private AtomicLong totalLoadTimeInMillis = new AtomicLong(0);
	private CircularList<Long> lastHundredLoadTimes = new CircularList<>(new ArrayList<>(100));
	
	public void incHitCounter() {hitCounter.incrementAndGet();}
	public void incMissCounter() {missCounter.incrementAndGet();}
	public void incLoadCounter(long loadTime) {
		totalLoadCounter.incrementAndGet();
		totalLoadTimeInMillis.addAndGet(loadTime);
		lastHundredLoadTimes.addElement(loadTime);
	}
	
	public void updateHitCounter(long hits) {
		hitCounter.addAndGet(hits);
	}

	public void updateMissCounter(long misses) {
		missCounter.addAndGet(misses);
	}	

}
