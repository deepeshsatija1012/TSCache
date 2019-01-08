package com.bfm.app.timeseries;

import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
	long startTime;
	/**
	 * End time of time series as stored in data store
	 */
	long endTime;
	/**
	 * Start time of time series for cache storage(only used for caching operations when we want to store subset of all time series for key)
	 */
	long storedEndTime;
	/**
	 * End time of time series for cache storage(only used for caching operations when we want to store subset of all time series for key)
	 */
	long storedStartTime;
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

	public long getStartTime() {
		return startTime;
	}

	public long getStoredStartTime() {
		return storedStartTime;
	}
	
	public long getEndTime() {
		return endTime;
	}

	public long getStoredEndTime() {
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
	public TimeSeriesEntry sample(long start, long end, IntervalType interval) {
		return null;
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
	public TimeSeriesEntry sample(long start, long end, IntervalType interval, 
			long unitJump, ChronoField unit) {
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
	 * @return
	 */
	public TimeSeriesEntry apply(long start, long end, IntervalType interval, 
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
	public TimeSeriesEntry apply(long start, long end, IntervalType interval, 
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
	public TimeSeriesEntry roll(long start, long end, IntervalType interval, 
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
	public TimeSeriesEntry applyWithFilter(long start, long end, IntervalType interval, 
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
	public TimeSeriesEntry applyWithFilter(long start, long end, IntervalType interval, 
			Map<String, Function> fieldBasedFunctionMap,
			Map<String, Predicate> fieldPredicateMap,
			long increment, ChronoField unit) {
		return null;
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
