package com.bfm.app.timeseries;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.bfm.app.timeseries.classifiers.IntervalType;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;

public class TimeSeriesEntryUtils {
	
	public static Status getStatus(TimeSeriesEntry ts) {
		return ts.getStatus();
	}
	
	public static boolean updateStatus(TimeSeriesEntry ts) {
		return ts.updatStatus();
	}
	
	public static boolean canUseThis(TimeSeriesEntry ts, LocalDateTime start, LocalDateTime end) {
		if(ts.getStatus()==Status.JUST_LOADED) {
			return true;
		}else {
			if(start.compareTo(ts.getStoredStartTime())<0 || end.compareTo(ts.getStoredEndTime())>0) {
				return false;
			}else {
				return true;
			}
		}
	}
	
	public static <T extends TimeSeriesEntry> T restrictedClone(T original, LocalDateTime start, LocalDateTime end, IntervalType interval, 
			TimeSeriesEntryBuilder<T> builder, Map<String, Class<?>> fieldTypeMap, Supplier<T> objectSupplier) {
		if(original.startTime.compareTo(start)>=0 && original.endTime.compareTo(end)<=0) {
			return original;
		}
		builder.startTime(original.startTime);builder.endTime(original.endTime);
		builder.storedStartTime(original.storedStartTime.compareTo(start)>0?original.storedStartTime:start);builder.storedEndTime(original.storedEndTime.compareTo(end)<0?original.storedEndTime:end);
		builder.fieldTypeMap(fieldTypeMap);
		builder.objectBuilder(objectSupplier);
		T newValue = builder.build();
		int startIndex = interval.getIndex(start, original.startTime), endIndex = interval.getIndex(end, original.startTime)+1;
		for(Map.Entry<String, Class<?>> entry : fieldTypeMap.entrySet()) {
			if(entry.getValue()==int.class || entry.getValue()==Integer.class) {
				TIntArrayList intValues = original.getIntField(entry.getKey());
				newValue.integerTimeSeriesFields.put(entry.getKey(), subList(intValues, startIndex, endIndex));
			}else if(entry.getValue()==long.class || entry.getValue()==Long.class) {
				TLongArrayList longValues = original.getLongField(entry.getKey());
				newValue.longTimeSeriesFields.put(entry.getKey(), subList(longValues, startIndex, endIndex));
			}else if(entry.getValue()==double.class || entry.getValue()==Double.class) {
				TDoubleArrayList doubleValues = original.getDoubleField(entry.getKey());
				newValue.doubleTimeSeriesFields.put(entry.getKey(), subList(doubleValues, startIndex, endIndex));
			}else if(entry.getValue()==String.class) {
				List<String> stringValues = original.getStringField(entry.getKey());
				newValue.stringTimeSeriesFields.put(entry.getKey(), subList(stringValues, startIndex, endIndex));
			}
		}
		original.updatStatus();newValue.updatStatus();
		
		return newValue;
	}
	
	public static TIntArrayList subList(TIntArrayList original, int begin, int end ) {
    	if ( end < begin ) {
			throw new IllegalArgumentException( "end index " + end +
				" greater than begin index " + begin );
		}
		if ( begin < 0 ) {
			throw new IndexOutOfBoundsException( "begin index can not be < 0" );
		}
        TIntArrayList list = new TIntArrayList( end - begin );
        for ( int i = Math.min(begin, 0); i < Math.min(original.size(), end); i++ ) {
        	list.add(original.get(i));
        }
        return list;
    }

	public static TDoubleArrayList subList(TDoubleArrayList original, int begin, int end ) {
    	if ( end < begin ) {
			throw new IllegalArgumentException( "end index " + end +
				" greater than begin index " + begin );
		}
		if ( begin < 0 ) {
			throw new IndexOutOfBoundsException( "begin index can not be < 0" );
		}
		TDoubleArrayList list = new TDoubleArrayList( end - begin );
        for ( int i = Math.min(begin, 0); i < Math.min(original.size(), end); i++ ) {
        	list.add(original.get(i));
        }
        return list;
    }
	
	public static TLongArrayList subList(TLongArrayList original, int begin, int end ) {
    	if ( end < begin ) {
			throw new IllegalArgumentException( "end index " + end +
				" greater than begin index " + begin );
		}
		if ( begin < 0 ) {
			throw new IndexOutOfBoundsException( "begin index can not be < 0" );
		}
		TLongArrayList list = new TLongArrayList( end - begin );
        for ( int i = Math.min(begin, 0); i < Math.min(original.size(), end); i++ ) {
        	list.add(original.get(i));
        }
        return list;
    }
	
	public static List<String> subList(List<String> original, int begin, int end){
		return new ArrayList<>(original.subList(Math.min(begin, 0), Math.min(original.size(), end)));
	}
}
