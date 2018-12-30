package com.bfm.app.timeseries;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;

public abstract class TimeSeriesEntry {
	String key;
	long actualOffSet;
	long lastEntry;
	long lastStoredEntry;
	long storedOffSet;
	private AtomicReference<Status> status = new AtomicReference<>(Status.JUST_LOADED);
	Map<String, TIntArrayList> integerTimeSeriesFields;
	Map<String, TLongArrayList> longTimeSeriesFields;
	Map<String, TDoubleArrayList> doubleTimeSeriesFields;
	Map<String, List<String>> stringTimeSeriesFields;
	
	public TimeSeriesEntry() {}
	
	TimeSeriesEntry(TimeSeriesEntry ts){
		this.key = ts.key;
		this.actualOffSet = ts.actualOffSet;
		this.lastEntry = ts.lastEntry;
		this.lastStoredEntry = ts.lastStoredEntry;
		this.storedOffSet = ts.storedOffSet;
	}
	
	public TIntArrayList getIntField(String name) {
		return integerTimeSeriesFields.get(name);
	}

	public TLongArrayList getLongField(String name) {
		return longTimeSeriesFields.get(name);
	}
	
	public TDoubleArrayList getDoubleField(String name) {
		return doubleTimeSeriesFields.get(name);
	}
	
	public List<String> getStringField(String name) {
		return stringTimeSeriesFields.get(name);
	}
	
	public Map<String, TIntArrayList> getIntegerTimeSeriesFields(){
		return Collections.unmodifiableMap(integerTimeSeriesFields);
	}

	public Map<String, TLongArrayList> getLongTimeSeriesFields(){
		return Collections.unmodifiableMap(longTimeSeriesFields);
	}
	public Map<String, TDoubleArrayList> getDoubleTimeSeriesFields(){
		return Collections.unmodifiableMap(doubleTimeSeriesFields);
	}
	public Map<String, List<String>> getStringTimeSeriesFields(){
		return Collections.unmodifiableMap(stringTimeSeriesFields);
	}
	public String getKey() {
		return this.key;
	}
	public void setKey(String key) {
		this.key = key;
	}

	public long getActualOffSet() {
		return actualOffSet;
	}

	public long getStoredOffSet() {
		return storedOffSet;
	}
	
	public long getLastEntry() {
		return lastEntry;
	}

	public long getLastStoredEntry() {
		return lastStoredEntry;
	}

	Status getStatus() {
		return this.status.get();
	}
	
	boolean updatStatus() {
		return this.status.compareAndSet(Status.JUST_LOADED, Status.LOADED);
	}
	
	
//	private static final Unsafe unsafe;
//	private static final long statusField;
//	
//	private static Unsafe getUnsafe() {
//        try {
//            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
//            theUnsafe.setAccessible(true);
//            return (Unsafe) theUnsafe.get(null);
//        } catch (Exception e) {
//            throw new AssertionError(e);
//        }
//    }
//	static {
//        try {
//        	unsafe = getUnsafe();
//            Class<?> k = TimeSeriesEntry.class;
//            statusField = unsafe.objectFieldOffset(k.getDeclaredField("status"));
//        } catch (Exception e) {
//            throw new Error(e);
//        }
//    }
}
