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
	private long startTimeInMillis = 0;
	public TimeSeriesEntryBuilder(String key, Supplier<T> objectBuilder) {
		this.key = key;
		this.objectBuilder = objectBuilder;
	}
	
	public TimeSeriesEntryBuilder<T> startTimeInMillis(long startTimeInMillis){
		this.startTimeInMillis = startTimeInMillis;
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
		tsEntry.storedOffSet = startTimeInMillis;
		
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
