package test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import com.bfm.app.timeseries.cache.EvictionStrategy;
import com.bfm.app.timeseries.cache.TSCache;
import com.bfm.app.timeseries.classifiers.IntervalType;
import com.bfm.app.timeseries.parametric.ParametricTimeSeries;

public class MainMultiThreadThreadLRU {
//	private static int getRandomNumberInRange(int min, int max) {
//		if (min >= max) {
//			throw new IllegalArgumentException("max must be greater than min");
//		}
//		Random r = new Random();
//		return r.nextInt((max - min) + 1) + min;
//	}
	
	public static void main(String[] args) throws InterruptedException {
		/*
		String cacheName, IntervalType intervalType, Function<K, D> singleEntryLoader,
		Function<Set<K>, Map<K, D>> multipleEntryLoader, Supplier<Map<K, D>> allEntriesLoader,
		Function<D, V> transformer, int allowedCacheSize, EvictionStrategy<K, V> evictionStrategy, 
		EvictionListener<K, V> listener, LocalDateTime startDate, LocalDateTime endDate, Map<String, Class<?>> fieldTypeMap,
		Supplier<V> objectSupplier
		*/
		Data dataProvider = new Data();
		Map<String, Class<?>> fieldTypeMap = new HashMap<>();
		fieldTypeMap.put("deltas", Double.class);
		fieldTypeMap.put("values", Double.class);
		Function<String, ParametricTimeSeries> single = s -> dataProvider.getTimeSeries(s);
		Function<Set<String>, Map<String, ParametricTimeSeries>> multipleEntryLoader = s -> dataProvider.getTimeSeries(s);
		Supplier<Map<String, ParametricTimeSeries>> allEntriesLoader = () -> dataProvider.getAll();
		Function<ParametricTimeSeries, ParametricTimeSeries> transformer = d -> d;
		AtomicInteger count = new AtomicInteger(0);
		EvictionStrategy<String, ParametricTimeSeries> evictionStartegy = new EvictionStrategy.LFUEvictionStrategy<>();
//		List<String> evictionOrder = Lists.newArrayList("10001", "10002", "10004", "10005", "10006", "10009");
		TSCache<String, ParametricTimeSeries, ParametricTimeSeries> cache = 
				new TSCache<>("parametric", IntervalType.DAILY, single, multipleEntryLoader, allEntriesLoader, 
						transformer, 10, evictionStartegy,
						(key, value) -> System.out.println(key+ " ["+count.get()+"]"+ evictionStartegy.queue()),
						null, null, fieldTypeMap, ParametricTimeSeries::new);
		
		
		List<String> keys = new ArrayList<>(20);
		keys.add("10000");//NONE  [00]10000
		keys.add("10001");//NONE  [01]10000->10001
		keys.add("10002");//NONE  [02]10000->10001->10002
		keys.add("10003");//NONE  [03]10000->10001->10002->10003
		keys.add("10004");//NONE  [04]10000->10001->10002->10003->10004
		keys.add("10005");//NONE  [05]10000->10001->10002->10003->10004->10005
		keys.add("10006");//NONE  [06]10000->10001->10002->10003->10004->10005->10006
		keys.add("10007");//NONE  [07]10000->10001->10002->10003->10004->10005->10006->10007
		keys.add("10008");//NONE  [08]10000->10001->10002->10003->10004->10005->10006->10007->10008
		keys.add("10000");//NONE  [09]10001->10002->10003->10004->10005->10006->10007->10008->10000
		keys.add("10009");//NONE  [10]10001->10002->10003->10004->10005->10006->10007->10008->10000->10009
		keys.add("11000");//10001 [11]10002->10003->10004->10005->10006->10007->10008->10000->10009->11000
		keys.add("10003");//NONE  [12]10002->10004->10005->10006->10007->10008->10000->10009->11000->10003
		keys.add("11001");//10002 [13]10004->10005->10006->10007->10008->10000->10009->11000->10003->11001
		keys.add("11002");//10004 [14]10005->10006->10007->10008->10000->10009->11000->10003->11002->11002
		keys.add("11003");//10005 [15]10006->10007->10008->10000->10009->11000->10003->11002->11002->11003
		keys.add("11004");//10006 [16]10007->10008->10000->10009->11000->10003->11002->11002->11003->11004
		keys.add("11005");//10007 [17]10008->10000->10009->11000->10003->11002->11002->11003->11004->11005
		keys.add("11006");//10008 [18]10000->10009->11000->10003->11002->11002->11003->11004->11005->11006
		keys.add("11007");//10000 [19]10009->11000->10003->11002->11002->11003->11004->11005->11006->11007
		
		ThreadPoolExecutor executor = new ThreadPoolExecutor(20, 20, Long.MAX_VALUE, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1, false), new ThreadPoolExecutor.CallerRunsPolicy());
		for(String key : keys) {
			executor.execute(() -> { 
				System.out.println("["+ count.getAndIncrement() + "] cache.get("+key+") "+evictionStartegy.queue() );
				cache.get(key);
			});
		}
		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		
		System.out.println(evictionStartegy.queue());
	}

}
