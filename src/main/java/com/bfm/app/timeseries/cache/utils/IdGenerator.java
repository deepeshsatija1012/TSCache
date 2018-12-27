package com.bfm.app.timeseries.cache.utils;

import java.util.concurrent.atomic.AtomicLong;

public class IdGenerator {
	
	private static final AtomicLong SEQUENCE = new AtomicLong(0);
	
	public static long getNextInSequence() {
		return SEQUENCE.incrementAndGet();
	}

}
