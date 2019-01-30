package com.bfm.app.timeseries.classifiers;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.Sets;

public abstract class IntervalType {
	public static final IntervalType DAILY = new DailyInterval(null, null); 
	
	
	private TreeMap<LocalDateTime, Integer> intervalRange = new TreeMap<>();
	private ReentrantLock lock = new ReentrantLock();
	private Set<LocalDateTime> holidays;
	private Set<DayOfWeek> weekends;
	
	public IntervalType(Collection<LocalDateTime> holidays, Collection<DayOfWeek> weekends) {
		if(CollectionUtils.isEmpty(holidays)) {
			this.holidays = Sets.newHashSet();
		}else {
			this.holidays = new TreeSet<>();
			this.holidays.addAll(holidays);
		}
		
		if(CollectionUtils.isEmpty(weekends)) {
			this.weekends = Sets.newHashSet(DayOfWeek.SUNDAY, DayOfWeek.SATURDAY);
		}else {
			this.weekends = Sets.newHashSet(weekends); 
		}
	}
	
	protected void populateIntervals() {
		LocalDateTime start = LocalDateTime.of(1950, 01, 1, 0, 0);
		LocalDateTime end = LocalDateTime.of(2222,12, 31, 0, 0);
		int index = 0;
		while(!start.isEqual(end)) {
			if(!this.getWeekends().contains(start.getDayOfWeek())) {
				if(!this.getHolidays().contains(start))
					this.addEntryToInterval(start, index++);
			}
			start = start.plusDays(1);
		}
	}
	
	public boolean isHoliday(LocalDateTime time) {
		return this.holidays.contains(time) || isWeekend(time);
	}
	
	public boolean isWeekend(LocalDateTime time) {
		return weekends.contains(time.getDayOfWeek());
	}
	
	public Set<LocalDateTime> getHolidays() {
		return Collections.unmodifiableSet(holidays);
	}

	public Set<DayOfWeek> getWeekends() {
		return Collections.unmodifiableSet(weekends);
	}
	
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
	
	public int getIndex(LocalDateTime time, LocalDateTime timeOffset) {
		if(intervalRange.size()==0) {
			initialize();
		}
		try {
			return intervalRange.get(time)-intervalRange.get(timeOffset);
		}catch (NullPointerException e) {
			return -1;
		}
	}
	
	public int getClosetPreviousIndex(LocalDateTime time, LocalDateTime timeOffset) {
		if(intervalRange.size()==0) {
			initialize();
		}
		return intervalRange.floorEntry(time).getValue()-intervalRange.floorEntry(timeOffset).getValue();
	}
	
	
	public int getClosetNextIndex(LocalDateTime time, LocalDateTime timeOffset) {
		if(intervalRange.size()==0) {
			initialize();
		}
		return intervalRange.ceilingEntry(time).getValue()-intervalRange.ceilingEntry(timeOffset).getValue();
	}
	
	public LocalDateTime getClosestTimePrevTo(LocalDateTime time) {
		return intervalRange.floorEntry(time).getKey();
	}
	
	public LocalDateTime getClosestTimeNextTo(LocalDateTime time) {
		return intervalRange.ceilingEntry(time).getKey();
	}
	
	protected void addEntryToInterval(LocalDateTime timeInMillis, Integer index) {
		intervalRange.put(timeInMillis, index);
	}
	
	public Map<LocalDateTime, Integer> getIntervalRange(){
		return Collections.unmodifiableMap(intervalRange);
	}
	
	public Map<Integer, Long> getIntervalRange(LocalDateTime start, LocalDateTime end, LocalDateTime timeOffset, IntervalType interval) {
		if(intervalRange.size()==0) {
			initialize();
		}
		Map<Integer, Long> map = new TreeMap<>();
//		int startIndex = getClosetNextIndex(start, timeOffset), endIndex = getClosetPreviousIndex(end, timeOffset);
		LocalDateTime startDateTime = getClosestTimeNextTo(interval.getClosestTimeNextTo(start)), endDateTime = getClosestTimePrevTo(interval.getClosestTimePrevTo(end));
		
		while(startDateTime.compareTo(endDateTime)<0) {
			Long time = startDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
			if(!interval.isHoliday(startDateTime) && !this.isHoliday(startDateTime)) {
				int index = getIndex(startDateTime, timeOffset);
				map.put(index, time);
			}
			startDateTime = getNext(startDateTime);
		}
		return map;
	}
	
	public abstract LocalDateTime getNext(LocalDateTime localDate) ;
	
	public abstract String format(long time);
	
	public static class DailyInterval extends IntervalType {
		public DailyInterval(Collection<LocalDateTime> holidays, Collection<DayOfWeek> weekends) {
			super(holidays, weekends);
		}
		
		protected void populateHolidays(Collection<LocalDateTime> holidays, Set<Long> holidaySet) {
			for(LocalDateTime d : holidays) {
				holidaySet.add(d.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
			}
		}

		@Override
		protected void populateIntervals() {
			LocalDateTime start = LocalDateTime.of(1950, 01, 1, 0, 0);
			LocalDateTime end = LocalDateTime.of(2222,12, 31, 0, 0);
			int index = 0;
			while(!start.isEqual(end)) {
				if(!this.getWeekends().contains(start.getDayOfWeek())) {
					if(!this.getHolidays().contains(start))
						this.addEntryToInterval(start, index++);
				}
				start = getNext(start);
			}
		}

		@Override
		public LocalDateTime getNext(LocalDateTime localDate) {
			return localDate.plusDays(1);
		}

		private  static final DateTimeFormatter FROMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
		@Override
		public String format(long time) {
			return FROMAT.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault()));
		}
	}
	
	
}
