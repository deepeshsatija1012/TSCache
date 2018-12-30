package com.bfm.app.timeseries;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimeSeriesEntryBuilder<T extends TimeSeriesEntry> {
	private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesEntryBuilder.class);
	private Map<String, Class<?>> fieldTypeMap = new HashMap<>();
	private String key;
	private Supplier<T> objectBuilder;
	private long actualOffSet;
	private long lastEntry;
	private long lastStoredEntry;
	private long storedOffSet;
	
	public TimeSeriesEntryBuilder<T> reset() {
		key = null; objectBuilder=null;
		fieldTypeMap.clear();
		actualOffSet = lastEntry = lastStoredEntry = storedOffSet = 0L;
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
	
	public TimeSeriesEntryBuilder<T> actualOffSet(long actualOffSet){
		this.actualOffSet = actualOffSet;
		return this;
	}

	public TimeSeriesEntryBuilder<T> lastEntry(long lastEntry){
		this.lastEntry = lastEntry;
		return this;
	}
	
	public TimeSeriesEntryBuilder<T> lastStoredEntry(long lastStoredEntry){
		this.lastStoredEntry = lastStoredEntry;
		return this;
	}
	
	public TimeSeriesEntryBuilder<T> storedOffSet(long storedOffSet){
		this.storedOffSet = storedOffSet;
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
		T tsEntry = objectBuilder.get();
		tsEntry.actualOffSet = actualOffSet;
		tsEntry.lastEntry = lastEntry;
		tsEntry.storedOffSet = storedOffSet;
		tsEntry.lastStoredEntry = lastStoredEntry;
		
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
