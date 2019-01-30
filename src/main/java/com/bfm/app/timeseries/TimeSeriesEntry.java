package com.bfm.app.timeseries;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

import com.bfm.app.timeseries.classifiers.IntervalType;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;


/**
 * This object stores the time series of fields of following types :<br/>
 * 	1. INTEGER; 2. LONG; 3. DOUBLE; 4. STRING<br/>
 * <br/>
 * lets say we have a time series for a Key 'K' with following field, with dates 2018-12-03 to 2018-12-31<br/>
 * 	<li> delta - DOUBLE</li><br/>
 * 	<li> value - DOUBLE</li>
 * <br/>
 * The structure would be like<br/>
 * <pre>
 * <code>
 * TimeSeriesEntry {
 * 	key = "K"
 * 	startTime = 2018-12-03(in millis since epoch)
 * 	endTime   = 2018-12-31(in millis since epoch)
 *	storedStartTime = 2018-12-03(in millis since epoch)
 * 	storedEndTime   = 2018-12-31(in millis since epoch)
 * 	integerTimeSeriesFields = NULL
 * 	longTimeSeriesFields = NULL
 * 	doubleTimeSeriesFields = {
 * 			delta=[0.001, 0.0002.......],
 * 			value=[0.001, 0.0002.......]
 * 		}
 * }
 * </code>
 * </pre>
 * 
 * @author dsatija
 *
 */
public class TimeSeriesEntry {
	String key;
	/**
	 * Start time of the time series as stored in data store
	 */
	LocalDateTime startTime;
	/**
	 * End time of time series as stored in data store
	 */
	LocalDateTime endTime;
	/**
	 * Start time of time series for cache storage(only used for caching operations when we want to store subset of all time series for key)
	 */
	LocalDateTime storedEndTime;
	/**
	 * End time of time series for cache storage(only used for caching operations when we want to store subset of all time series for key)
	 */
	LocalDateTime storedStartTime;
	/**
	 * Used to implement multithreaded caching strategy
	 */
	private AtomicReference<Status> status = new AtomicReference<>(Status.JUST_LOADED);
	/**
	 * List if fields in Time Storing having Integer values
	 */
	Map<String, TIntArrayList> integerTimeSeriesFields;
	/**
	 * List if fields in Time Storing having Long values
	 */
	Map<String, TLongArrayList> longTimeSeriesFields;
	/**
	 * List if fields in Time Storing having Double values
	 */
	Map<String, TDoubleArrayList> doubleTimeSeriesFields;
	/**
	 * List if fields in Time Storing having String values
	 */
	Map<String, List<String>> stringTimeSeriesFields;
	/**
	 * Regular Interval object for generated time series
	 */
	IntervalType interval;
	
	public TimeSeriesEntry() {}
	
	TimeSeriesEntry(TimeSeriesEntry ts){
		this.key = ts.key;
		this.startTime = ts.startTime;
		this.endTime = ts.endTime;
		this.storedEndTime = ts.storedEndTime;
		this.storedStartTime = ts.storedStartTime;
		ts.interval = interval;
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

	public LocalDateTime getStartTime() {
		return startTime;
	}

	public LocalDateTime getStoredStartTime() {
		return storedStartTime;
	}
	
	public LocalDateTime getEndTime() {
		return endTime;
	}

	public LocalDateTime getStoredEndTime() {
		return storedEndTime;
	}

	Status getStatus() {
		return this.status.get();
	}
	
	boolean updatStatus() {
		return this.status.compareAndSet(Status.JUST_LOADED, Status.LOADED);
	}
	
	/**
	 * <pre>
	 * Get sub sequence of time series based on <code>start</code> & <code>end</code> parameters
	 * The <code>interval</code> can be used to define a new calendar specs which are different
	 * from the original TimeSeries
	 * 
	 * the function returns a new  {@code TimeSeriesEntry} with 
	 * 		<code>start >= current time series start time</code>
	 * 		<code>end <= current time series end time</code>
	 * </pre>
	 * @param start start time since epoch from where you wish to retrieve the time series
	 * @param end end time since epoch untill which wish to retrieve the time series
	 * @param interval the calender 
	 * @return
	 */
	public TimeSeriesEntry sample(LocalDateTime start, LocalDateTime end, IntervalType interval) {
		int actualStartIndex = this.interval.getClosetNextIndex(start, this.storedStartTime);
		LocalDateTime actualStartTime = this.interval.getClosestTimeNextTo(start);
		int actualEndIndex = this.interval.getClosetPreviousIndex(end, this.storedStartTime);
		LocalDateTime actualEndTime = this.interval.getClosestTimePrevTo(end);
		TimeSeriesEntry modified =  new TimeSeriesEntry();
		modified.startTime = this.startTime; modified.endTime = this.endTime;
		modified.storedStartTime = actualStartTime; modified.storedEndTime = actualEndTime;
		modified.interval = interval; modified.key = this.key;
		Set<Integer> indexesForHolidays = new HashSet<>();
		for(LocalDateTime holiday : interval.getHolidays()) {
			if(holiday.compareTo(storedStartTime)>=0 && holiday.compareTo(storedEndTime)<0) {
				int index = this.interval.getIndex(holiday, storedStartTime);
				if(index!=-1) {
					indexesForHolidays.add(index);
				}
			}
		}
		if(doubleTimeSeriesFields!=null) {
			for(Map.Entry<String, TDoubleArrayList> doubleFieldTSEntry : doubleTimeSeriesFields.entrySet()) {
				TDoubleArrayList list = (TDoubleArrayList) doubleFieldTSEntry.getValue();
				TDoubleArrayList newList = new TDoubleArrayList(actualEndIndex-actualStartIndex) ;
				for(int i=actualStartIndex;i<actualEndIndex;i++) {
					if(!indexesForHolidays.contains(i))
						newList.add(list.get(i));
				}
				
				if(modified.doubleTimeSeriesFields==null) {
					modified.doubleTimeSeriesFields = new HashMap<>();
				}
				modified.doubleTimeSeriesFields.put(doubleFieldTSEntry.getKey(), newList);
			}
		}
		if(integerTimeSeriesFields!=null) {
			for(Map.Entry<String, TIntArrayList> integerFieldTSEntry : integerTimeSeriesFields.entrySet()) {
				TIntArrayList list = (TIntArrayList) integerFieldTSEntry.getValue();
				TIntArrayList newList = new TIntArrayList(actualEndIndex-actualStartIndex) ;
				for(int i=actualStartIndex;i<actualEndIndex;i++) {
					if(!indexesForHolidays.contains(i))
						newList.add(list.get(i));
				}
				if(modified.integerTimeSeriesFields==null) {
					modified.integerTimeSeriesFields = new HashMap<>();
				}
				modified.integerTimeSeriesFields.put(integerFieldTSEntry.getKey(), newList);
			}
		}
		if(longTimeSeriesFields!=null) {
			for(Map.Entry<String, TLongArrayList> longFieldTSEntry : longTimeSeriesFields.entrySet()) {
				TLongArrayList list = (TLongArrayList) longFieldTSEntry.getValue();
				TLongArrayList newList = new TLongArrayList(actualEndIndex-actualStartIndex) ;
				for(int i=actualStartIndex;i<actualEndIndex;i++) {
					if(!indexesForHolidays.contains(i))
						newList.add(list.get(i));
				}
				if(modified.longTimeSeriesFields==null) {
					modified.longTimeSeriesFields = new HashMap<>();
				}
				modified.longTimeSeriesFields.put(longFieldTSEntry.getKey(), newList);
			}
		}
		if(stringTimeSeriesFields!=null) {
			for(Map.Entry<String, List<String>> stringFieldTSEntry : stringTimeSeriesFields.entrySet()) {
				List<String> list = new ArrayList<>(stringFieldTSEntry.getValue().subList(actualStartIndex, actualEndIndex));
				List<String> newList = new ArrayList<>(actualEndIndex-actualStartIndex);
				for(int i=actualStartIndex;i<actualEndIndex;i++) {
					if(!indexesForHolidays.contains(i))
						newList.add(list.get(i));
				}
				if(modified.stringTimeSeriesFields==null) {
					modified.stringTimeSeriesFields = new HashMap<>();
				}
				modified.stringTimeSeriesFields.put(stringFieldTSEntry.getKey(), newList);
			}
		}
		return modified;
	}
	
	
	/**
	 * <pre>
	 * Get sub sequence of time series based on <code>start</code> & <code>end</code> parameters
	 * The <code>interval</code> can be used to define a new calendar specs which are different
	 * from the original TimeSeries
	 * 
	 * the function returns a new  {@code TimeSeriesEntry} with
	 * 		<code>start >= current time series start time</code>
	 * 		<code>end <= current time series end time</code><
	 * </pre>
	 * 
	 * @param start start time since epoch from where you wish to retrieve the time series
	 * @param end end time since epoch untill which wish to retrieve the time series
	 * @param interval the calender 
	 * @param unitJump the number of units to jump for next time series observation
	 * @param unit the unit definition, Days, Months, Seconds etc 
	 * @return
	 */
	public TimeSeriesEntry sample(LocalDateTime start, LocalDateTime end, IntervalType interval, 
			long unitJump, ChronoField unit) {
		int actualStartIndex = this.interval.getClosetNextIndex(start, this.storedStartTime);
		LocalDateTime actualStartTime = this.interval.getClosestTimeNextTo(start);
		int actualEndIndex = this.interval.getClosetPreviousIndex(end, this.storedStartTime);
		LocalDateTime actualEndTime = this.interval.getClosestTimePrevTo(end);
		TimeSeriesEntry modified =  new TimeSeriesEntry();
		modified.startTime = this.startTime; modified.endTime = this.endTime;
		modified.storedStartTime = actualStartTime; modified.storedEndTime = actualEndTime;
		modified.interval = interval; modified.key = this.key;
		Set<Integer> indexesForHolidays = new HashSet<>();
		for(LocalDateTime holiday : interval.getHolidays()) {
			int index = this.interval.getIndex(holiday, modified.storedStartTime);
			if(index!=-1) {
				indexesForHolidays.add(index);
			}
		}
		if(doubleTimeSeriesFields!=null) {
			for(Map.Entry<String, TDoubleArrayList> doubleFieldTSEntry : doubleTimeSeriesFields.entrySet()) {
				TDoubleArrayList list = (TDoubleArrayList) doubleFieldTSEntry.getValue();
				TDoubleArrayList newList = new TDoubleArrayList(actualEndIndex-actualStartIndex) ;
				for(int i=actualStartIndex;i<actualEndIndex;i++) {
					if(!indexesForHolidays.contains(i))
						newList.add(list.get(i));
				}
				
				if(modified.doubleTimeSeriesFields==null) {
					modified.doubleTimeSeriesFields = new HashMap<>();
				}
				modified.doubleTimeSeriesFields.put(doubleFieldTSEntry.getKey(), newList);
			}
		}
		if(integerTimeSeriesFields!=null) {
			for(Map.Entry<String, TIntArrayList> integerFieldTSEntry : integerTimeSeriesFields.entrySet()) {
				TIntArrayList list = (TIntArrayList) integerFieldTSEntry.getValue();
				TIntArrayList newList = new TIntArrayList(actualEndIndex-actualStartIndex) ;
				for(int i=actualStartIndex;i<actualEndIndex;i++) {
					if(!indexesForHolidays.contains(i))
						newList.add(list.get(i));
				}
				if(modified.integerTimeSeriesFields==null) {
					modified.integerTimeSeriesFields = new HashMap<>();
				}
				modified.integerTimeSeriesFields.put(integerFieldTSEntry.getKey(), newList);
			}
		}
		if(longTimeSeriesFields!=null) {
			for(Map.Entry<String, TLongArrayList> longFieldTSEntry : longTimeSeriesFields.entrySet()) {
				TLongArrayList list = (TLongArrayList) longFieldTSEntry.getValue();
				TLongArrayList newList = new TLongArrayList(actualEndIndex-actualStartIndex) ;
				for(int i=actualStartIndex;i<actualEndIndex;i++) {
					if(!indexesForHolidays.contains(i))
						newList.add(list.get(i));
				}
				if(modified.longTimeSeriesFields==null) {
					modified.longTimeSeriesFields = new HashMap<>();
				}
				modified.longTimeSeriesFields.put(longFieldTSEntry.getKey(), newList);
			}
		}
		if(stringTimeSeriesFields!=null) {
			for(Map.Entry<String, List<String>> stringFieldTSEntry : stringTimeSeriesFields.entrySet()) {
				List<String> list = new ArrayList<>(stringFieldTSEntry.getValue().subList(actualStartIndex, actualEndIndex));
				List<String> newList = new ArrayList<>(actualEndIndex-actualStartIndex);
				for(int i=actualStartIndex;i<actualEndIndex;i++) {
					if(!indexesForHolidays.contains(i))
						newList.add(list.get(i));
				}
				if(modified.stringTimeSeriesFields==null) {
					modified.stringTimeSeriesFields = new HashMap<>();
				}
				modified.stringTimeSeriesFields.put(stringFieldTSEntry.getKey(), newList);
			}
		}
		return modified;
	}
	
	/**
	 * <pre>
	 * Create a new time series with the function applied to each entry for a univariate TimeSeries
	 * and each row of a multivarite time series
	 * 
	 * the function returns a new  {@code TimeSeriesEntry} with 
	 * 		<code>start >= current time series start time</code>
	 * 		<code>end <= current time series end time</code>
	 * 
	 * with the applicable calendar defined by {@code IntervalType}
	 * </pre>
	 * @param start start time since epoch from where you wish to retrieve the time series
	 * @param end end time since epoch untill which wish to retrieve the time series
	 * @param interval the calender 
	 * @param fieldBasedFunctionMap the function to apply on each field, if not available identity
	 * 			function is used
	 * @return
	 */
	public TimeSeriesEntry apply(LocalDateTime start, LocalDateTime end, IntervalType interval, 
			@SuppressWarnings("rawtypes") Map<String, Function> fieldBasedFunctionMap) {
		return null;
	}
	
	/**
	 * <pre>
	 * Create a new time series with the function applied to each entry for a univariate TimeSeries
	 * and each row of a multivarite time series
	 * 
	 * the function returns a new  {@code TimeSeriesEntry} with 
	 * 		<code>start >= current time series start time</code>
	 * 		<code>end <= current time series end time</code>
	 * 
	 * with the applicable calendar defined by {@code IntervalType}
	 * </pre>
	 * @param start start time since epoch from where you wish to retrieve the time series
	 * @param end end time since epoch until which wish to retrieve the time series
	 * @param interval the calender 
	 * @param fieldBasedFunctionMap the function to apply on each field, if not available identity
	 * 			function is used
	 * @param increment the number of units to increment for next time series observation
	 * @return
	 */
	public TimeSeriesEntry apply(LocalDateTime start, LocalDateTime end, IntervalType interval, 
			@SuppressWarnings("rawtypes") Map<String, Function> fieldBasedFunctionMap,
			long increment, ChronoField unit) {
		return null;
	}
	
	/**
	 * <pre>
	 * Create a new rolling time series with the function applied to each entry for a univariate TimeSeries
	 * and each row of a multivarite time series
	 * 
	 * the function returns a new  {@code TimeSeriesEntry} with 
	 * 		<code>start >= current time series start time</code>
	 * 		<code>end <= current time series end time</code>
	 * 
	 * with the applicable calendar defined by {@code IntervalType}
	 * </pre>
	 * @param start start time since epoch from where you wish to retrieve the time series
	 * @param end end time since epoch until which wish to retrieve the time series
	 * @param interval the calender 
	 * @param fieldBasedAccumulatorMap the function to applies to time series during accumulation window
	 * @param windowSize the window size
	 * @return
	 */
	public TimeSeriesEntry roll(LocalDateTime start, LocalDateTime end, IntervalType interval, 
			@SuppressWarnings("rawtypes") Map<String, BinaryOperator> fieldBasedAccumulatorMap,
			long windowSize) {
		return null;
	}
	
	/**
	 * <pre>
	 * Create a new time series with the function applied to each entry for a univariate TimeSeries
	 * and each row of a multivarite time series
	 * 
	 * the function returns a new  {@code TimeSeriesEntry} with 
	 * 		<code>start >= current time series start time</code>
	 * 		<code>end <= current time series end time</code>
	 * 
	 * with the applicable calendar defined by {@code IntervalType}
	 * </pre>
	 * @param start start time since epoch from where you wish to retrieve the time series
	 * @param end end time since epoch untill which wish to retrieve the time series
	 * @param interval the calender 
	 * @param fieldBasedFunctionMap the function to apply on each field, if not available identity
	 * 			function is used
	 * @param fieldPredicateMap Map containing predicate to be applied to each field
	 * @return
	 */
	
	@SuppressWarnings("rawtypes")
	public TimeSeriesEntry applyWithFilter(LocalDateTime start, LocalDateTime end, IntervalType interval, 
			Map<String, Function> fieldBasedFunctionMap,
			Map<String, Predicate> fieldPredicateMap) {
		return null;
	}
	
	/**
	 * <pre>
	 * Create a new time series with the function applied to each entry for a univariate TimeSeries
	 * and each row of a multivarite time series
	 * 
	 * the function returns a new  {@code TimeSeriesEntry} with 
	 * 		<code>start >= current time series start time</code>
	 * 		<code>end <= current time series end time</code>
	 * 
	 * with the applicable calendar defined by {@code IntervalType}
	 * </pre<
	 * @param start start time since epoch from where you wish to retrieve the time series
	 * @param end end time since epoch until which wish to retrieve the time series
	 * @param interval the calender 
	 * @param fieldBasedFunctionMap the function to apply on each field, if not available identity
	 * 			function is used
	 * @param fieldPredicateMap Map containing predicate to be applied 
	 * @param increment the number of units to increment for next time series observation
	 * @param unit the unit definition, Days, Months, Seconds etc 
	 * @return
	 */
	@SuppressWarnings("rawtypes") 
	public TimeSeriesEntry applyWithFilter(LocalDateTime start, LocalDateTime end, IntervalType interval, 
			Map<String, Function> fieldBasedFunctionMap,
			Map<String, Predicate> fieldPredicateMap,
			long increment, ChronoField unit) {
		return null;
	}
	
	
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("key=").append(this.key).append(System.lineSeparator());
		builder.append("startTime=").append(this.startTime).append(", endTime=").append(this.endTime).append(System.lineSeparator());
		builder.append("storedStartTime=").append(this.storedStartTime).append(", storedEndTime=").append(this.storedEndTime).append(System.lineSeparator());
		Map<Integer, Long> indexMap = this.interval.getIntervalRange(this.storedStartTime, this.storedEndTime, this.storedStartTime, this.interval);
		if(doubleTimeSeriesFields!=null) {
			for(Map.Entry<String, TDoubleArrayList> doubleFieldTSEntry : doubleTimeSeriesFields.entrySet()) {
				builder.append("field = ").append(doubleFieldTSEntry.getKey()).append("[");
				for(int i=0;i<doubleFieldTSEntry.getValue().size();i++) {
					builder.append(this.interval.format(indexMap.get(i))).append("=").append(doubleFieldTSEntry.getValue().get(i)).append(", ");
				}
				builder.append("]").append(System.lineSeparator());
			}
		}
		if(integerTimeSeriesFields!=null) {
			for(Map.Entry<String, TIntArrayList> integerFieldTSEntry : integerTimeSeriesFields.entrySet()) {
				builder.append("field = ").append(integerFieldTSEntry.getKey()).append("[");
				for(int i=0;i<integerFieldTSEntry.getValue().size();i++) {
					builder.append(this.interval.format(indexMap.get(i))).append("=").append(integerFieldTSEntry.getValue().get(i)).append(", ");
				}
				builder.append("]").append(System.lineSeparator());
			}
		}
		if(longTimeSeriesFields!=null) {
			for(Map.Entry<String, TLongArrayList> longFieldTSEntry : longTimeSeriesFields.entrySet()) {
				builder.append("field = ").append(longFieldTSEntry.getKey()).append("[");
				for(int i=0;i<longFieldTSEntry.getValue().size();i++) {
					builder.append(this.interval.format(indexMap.get(i))).append("=").append(longFieldTSEntry.getValue().get(i)).append(", ");
				}
				builder.append("]").append(System.lineSeparator());
			}
		}
		
		if(stringTimeSeriesFields!=null) {
			for(Map.Entry<String, List<String>> stringFieldTSEntry : stringTimeSeriesFields.entrySet()) {
				builder.append("field = ").append(stringFieldTSEntry.getKey()).append("[");
				for(int i=0;i<stringFieldTSEntry.getValue().size();i++) {
					builder.append(this.interval.format(indexMap.get(i))).append("=").append(stringFieldTSEntry.getValue().get(i)).append(", ");
				}
				builder.append("]").append(System.lineSeparator());
			}
		}
		return builder.toString();
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
