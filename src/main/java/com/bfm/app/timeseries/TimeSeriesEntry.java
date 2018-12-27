package com.bfm.app.timeseries;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;

public abstract class TimeSeriesEntry {
	String key;
	long actualOffSet;
	int totalObservations;
	int storedObservations;
	long storedOffSet;
	Map<String, TIntArrayList> integerTimeSeriesFields;
	Map<String, TLongArrayList> longTimeSeriesFields;
	Map<String, TDoubleArrayList> doubleTimeSeriesFields;
	Map<String, List<String>> stringTimeSeriesFields;
	
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
}
