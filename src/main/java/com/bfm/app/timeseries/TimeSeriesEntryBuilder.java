package com.bfm.app.timeseries;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bfm.app.timeseries.classifiers.IntervalType;


public class TimeSeriesEntryBuilder<T extends TimeSeriesEntry> {
	private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesEntryBuilder.class);
	private static final Supplier<TimeSeriesEntry> TS_ENTRY_SUPPLIER = () -> new TimeSeriesEntry(); 
	
	private Map<String, Class<?>> fieldTypeMap = new HashMap<>();
	private String key;
	private Supplier<T> objectBuilder;
	private LocalDateTime startTime;
	private LocalDateTime endTime;
	private LocalDateTime storedEndTime;
	private LocalDateTime storedStartTime;
	private IntervalType interval;
	
	public TimeSeriesEntryBuilder<T> reset() {
		key = null; objectBuilder=null;
		fieldTypeMap.clear();
		startTime = endTime = storedEndTime = storedStartTime = null;
		interval = null;
		return this;
	}
	
	public TimeSeriesEntryBuilder<T> interval(IntervalType interval){
		this.interval = interval;
		return this;
	}
	
	public TimeSeriesEntryBuilder<T> key(String key){
		this.key = key;
		return this;
	}
	
	public TimeSeriesEntryBuilder<T> objectBuilder(Supplier<T> objectBuilder){
		this.objectBuilder = objectBuilder;
		return this;
	}
	
	public TimeSeriesEntryBuilder<T> startTime(long startTime){
		this.startTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(startTime), ZoneId.systemDefault());
		return this;
	}

	public TimeSeriesEntryBuilder<T> endTime(long endTime){
		this.endTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(endTime), ZoneId.systemDefault());;
		return this;
	}
	
	public TimeSeriesEntryBuilder<T> storedEndTime(long storedEndTime){
		this.storedEndTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(storedEndTime), ZoneId.systemDefault());;
		return this;
	}
	
	public TimeSeriesEntryBuilder<T> storedStartTime(long storedStartTime){
		this.storedStartTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(storedStartTime), ZoneId.systemDefault());;
		return this;
	}
	
	public TimeSeriesEntryBuilder<T> startTime(LocalDateTime startTime){
		this.startTime = startTime;
		return this;
	}

	public TimeSeriesEntryBuilder<T> endTime(LocalDateTime endTime){
		this.endTime = endTime;
		return this;
	}
	
	public TimeSeriesEntryBuilder<T> storedEndTime(LocalDateTime storedEndTime){
		this.storedEndTime = storedEndTime;
		return this;
	}
	
	public TimeSeriesEntryBuilder<T> storedStartTime(LocalDateTime storedStartTime){
		this.storedStartTime = storedStartTime;
		return this;
	}
	
	public TimeSeriesEntryBuilder<T> fieldTypeMap(Map<String, Class<?>> fieldTypeMap){
		this.fieldTypeMap.putAll(fieldTypeMap);
		return this;
	}
	
	public TimeSeriesEntryBuilder<T> addField(String name, Class<?> clazz) {
		if(fieldTypeMap.containsKey(name)) {
			LOGGER.warn("Field {} is already defined of type {}, overriding with type {}", name, fieldTypeMap.get(name), clazz);
		}
		fieldTypeMap.put(name, clazz);
		return this;
	}
	
	public T build() {
		@SuppressWarnings("unchecked")
		T tsEntry = objectBuilder==null?((T) TS_ENTRY_SUPPLIER.get()):objectBuilder.get();
		tsEntry.startTime = startTime;
		tsEntry.endTime = endTime;
		tsEntry.storedStartTime = storedStartTime;
		tsEntry.storedEndTime = storedEndTime;
		tsEntry.interval = interval;
		
		tsEntry.key = this.key;
		for(Map.Entry<String, Class<?>> entry : fieldTypeMap.entrySet()) {
			if(entry.getValue()==int.class || entry.getValue()==Integer.class) {
				if(tsEntry.integerTimeSeriesFields==null) {
					tsEntry.integerTimeSeriesFields = new HashMap<>();
				}
				tsEntry.integerTimeSeriesFields.put(entry.getKey(), TimeSeriesFieldTypeSuppliers.INTEGER_TYPE_SUPPLIER.get());
			}else if(entry.getValue()==long.class || entry.getValue()==Long.class) {
				if(tsEntry.longTimeSeriesFields==null) {
					tsEntry.longTimeSeriesFields = new HashMap<>();
				}
				tsEntry.longTimeSeriesFields.put(entry.getKey(), TimeSeriesFieldTypeSuppliers.LONG_TYPE_SUPPLIER.get());
			}else if(entry.getValue()==double.class || entry.getValue()==Double.class) {
				if(tsEntry.doubleTimeSeriesFields==null) {
					tsEntry.doubleTimeSeriesFields = new HashMap<>();
				}
				tsEntry.doubleTimeSeriesFields.put(entry.getKey(), TimeSeriesFieldTypeSuppliers.DOUBLE_TYPE_SUPPLIER.get());
			}else if(entry.getValue()==String.class) {
				if(tsEntry.stringTimeSeriesFields==null) {
					tsEntry.stringTimeSeriesFields = new HashMap<>();
				}
				tsEntry.stringTimeSeriesFields.put(entry.getKey(), TimeSeriesFieldTypeSuppliers.STRING_TYPE_SUPPLIER.get());
			}
		}
		return tsEntry;
	}

}
