package com.bfm.app.timeseries.classifiers;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

public abstract class IntervalType {
	public static final IntervalType DAILY = new DailyInterval(); 
	
	
	private TreeMap<Long, Integer> intervalRange = new TreeMap<>();
	private ReentrantLock lock = new ReentrantLock();
	
	protected abstract void populateIntervals();
	
	public IntervalType initialize() {
		lock.lock();
		try {
			if(intervalRange.size()==0) {
				populateIntervals();
			}
		}finally {
			lock.unlock();
		}
		return this;
	}
	
	public int getIndex(long time, long timeOffset) {
		if(intervalRange.size()==0) {
			populateIntervals();
		}
		return intervalRange.get(time)-intervalRange.get(timeOffset);
	}
	
	protected void addEntryToInterval(Long timeInMillis, Integer index) {
		intervalRange.put(timeInMillis, index);
	}
	
	public Map<Long, Integer> getIntervalRange(){
		return Collections.unmodifiableMap(intervalRange);
	}
	
	
	public static class DailyInterval extends IntervalType {
		@Override
		protected void populateIntervals() {
			
			LocalDate startDate = LocalDate.of(1950, 1, 01);
			LocalDate endDate = LocalDate.now();
			int index = 0;
			while(!startDate.isEqual(endDate)) {
				if(startDate.getDayOfWeek()!=DayOfWeek.SUNDAY && startDate.getDayOfWeek()!=DayOfWeek.SATURDAY) {
					Long time = startDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
					this.addEntryToInterval(time, index++);
				}
				startDate = startDate.plus(1, ChronoUnit.DAYS);
			}
		}
	}

}
