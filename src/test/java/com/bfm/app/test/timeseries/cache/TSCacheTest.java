package com.bfm.app.test.timeseries.cache;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import com.bfm.app.timeseries.cache.EvictionStrategy;
import com.bfm.app.timeseries.cache.TSCache;
import com.bfm.app.timeseries.classifiers.IntervalType;
import com.bfm.app.timeseries.parametric.ParametricTimeSeries;

import test.Data;

public class TSCacheTest {
	
	@Test
	public void testLRU() {
		Data dataProvider = new Data();
		LocalDate today = LocalDate.now();
		for(int i=0;i<2520;i++) {
			if(today.getDayOfWeek()!=DayOfWeek.SATURDAY && today.getDayOfWeek()!=DayOfWeek.SUNDAY) {
				today = today.plusDays(-1);
			}
		}
		Function<String, ParametricTimeSeries> single = s -> dataProvider.getTimeSeries(s);
		Function<Set<String>, Map<String, ParametricTimeSeries>> multipleEntryLoader = s -> dataProvider.getTimeSeries(s);
		Supplier<Map<String, ParametricTimeSeries>> allEntriesLoader = () -> dataProvider.getAll();
		Function<ParametricTimeSeries, ParametricTimeSeries> transformer = d -> d;
		AtomicInteger count = new AtomicInteger(0);
		Map<String, Class<?>> fieldTypeMap = new HashMap<>();
		fieldTypeMap.put("deltas", Double.class);
		fieldTypeMap.put("values", Double.class);
		AtomicReference<String> keyUsed = new AtomicReference<String>("");
		TSCache<String, ParametricTimeSeries, ParametricTimeSeries> cache = 
				new TSCache<>("parametric", IntervalType.DAILY, single, multipleEntryLoader, allEntriesLoader, 
						transformer, 10, new EvictionStrategy.LRUEvictionStrategy<>(),
						(key, value) -> {
							String[] arr = keyUsed.get().split(",");
							Assert.assertTrue("Key : "+ arr[0] +" E : " +arr[1]+ " A : "+ key, arr[1].equals(key));
							System.out.println("At count "+ count.get() + " Evicted Key : "+ key);
						}, today.atStartOfDay(), LocalDate.now().atStartOfDay(), fieldTypeMap,
						ParametricTimeSeries::new
						);
		
		List<String> keys = new ArrayList<>(20);
		keys.add("10000,null");//NONE  [00]10000
		keys.add("10001,null");//NONE  [01]10000->10001
		keys.add("10002,null");//NONE  [02]10000->10001->10002
		keys.add("10003,null");//NONE  [03]10000->10001->10002->10003
		keys.add("10004,null");//NONE  [04]10000->10001->10002->10003->10004
		keys.add("10005,null");//NONE  [05]10000->10001->10002->10003->10004->10005
		keys.add("10006,null");//NONE  [06]10000->10001->10002->10003->10004->10005->10006
		keys.add("10007,null");//NONE  [07]10000->10001->10002->10003->10004->10005->10006->10007
		keys.add("10008,null");//NONE  [08]10000->10001->10002->10003->10004->10005->10006->10007->10008
		keys.add("10000,null");//NONE  [09]10001->10002->10003->10004->10005->10006->10007->10008->10000
		keys.add("10009,null");//NONE  [10]10001->10002->10003->10004->10005->10006->10007->10008->10000->10009
		keys.add("11000,10001");//10001 [11]10002->10003->10004->10005->10006->10007->10008->10000->10009->11000
		keys.add("10003,null");//NONE  [12]10002->10004->10005->10006->10007->10008->10000->10009->11000->10003
		keys.add("11001,10002");//10002 [13]10004->10005->10006->10007->10008->10000->10009->11000->10003->11001
		keys.add("11002,10004");//10004 [14]10005->10006->10007->10008->10000->10009->11000->10003->11002->11002
		keys.add("11003,10005");//10005 [15]10006->10007->10008->10000->10009->11000->10003->11002->11002->11003
		keys.add("11004,10006");//10006 [16]10007->10008->10000->10009->11000->10003->11002->11002->11003->11004
		keys.add("11005,10007");//10007 [17]10008->10000->10009->11000->10003->11002->11002->11003->11004->11005
		keys.add("11006,10008");//10008 [18]10000->10009->11000->10003->11002->11002->11003->11004->11005->11006
		keys.add("11007,10000");//10000 [19]10009->11000->10003->11002->11002->11003->11004->11005->11006->11007
		
		for(String key : keys) {
			keyUsed.set(key);
			cache.get(key.split(",")[0]);
			count.incrementAndGet();
		}
	}

	@Test
	public void testLFU() {
		Data dataProvider = new Data();
		LocalDate today = LocalDate.now();
		for(int i=0;i<2520;i++) {
			if(today.getDayOfWeek()!=DayOfWeek.SATURDAY && today.getDayOfWeek()!=DayOfWeek.SUNDAY) {
				today = today.plusDays(-1);
			}
		}
		Function<String, ParametricTimeSeries> single = s -> dataProvider.getTimeSeries(s);
		Function<Set<String>, Map<String, ParametricTimeSeries>> multipleEntryLoader = s -> dataProvider.getTimeSeries(s);
		Supplier<Map<String, ParametricTimeSeries>> allEntriesLoader = () -> dataProvider.getAll();
		Function<ParametricTimeSeries, ParametricTimeSeries> transformer = d -> d;
		AtomicInteger count = new AtomicInteger(0);
		Map<String, Class<?>> fieldTypeMap = new HashMap<>();
		fieldTypeMap.put("deltas", Double.class);
		fieldTypeMap.put("values", Double.class);
		AtomicReference<String> keyUsed = new AtomicReference<String>("");
		TSCache<String, ParametricTimeSeries, ParametricTimeSeries> cache = 
				new TSCache<>("parametric", IntervalType.DAILY, single, multipleEntryLoader, allEntriesLoader, 
						transformer, 10, new EvictionStrategy.LFUEvictionStrategy<>(),
						(key, value) -> {
							String[] arr = keyUsed.get().split(",");
							Assert.assertTrue("Key : "+ arr[0] +" E : " +arr[1]+ " A : "+ key, arr[1].equals(key));
							System.out.println("At count "+ count.get() + " Evicted Key : "+ key);
						}, today.atStartOfDay(), LocalDate.now().atStartOfDay(), fieldTypeMap,
						ParametricTimeSeries::new
				);
			
		List<String> keys = new ArrayList<>(20);
		keys.add("10000,null");//NONE   [00]10000[1][2]
		keys.add("10001,null");//NONE   [01]10000[1][2]->10001[1][4]
		keys.add("10002,null");//NONE   [02]10000[1][2]->10001[1][4]->10002[1][6]
		keys.add("10003,null");//NONE   [03]10000[1][2]->10001[1][4]->10002[1][6]->10003[1][8]
		keys.add("10004,null");//NONE   [04]10000[1][2]->10001[1][4]->10002[1][6]->10003[1][8]->10004[1][10]
		keys.add("10005,null");//NONE   [05]10000[1][2]->10001[1][4]->10002[1][6]->10003[1][8]->10004[1][10]->10005[1][12]
		keys.add("10006,null");//NONE   [06]10000[1][2]->10001[1][4]->10002[1][6]->10003[1][8]->10004[1][10]->10005[1][12]->10006[1][14]
		keys.add("10007,null");//NONE   [07]10000[1][2]->10001[1][4]->10002[1][6]->10003[1][8]->10004[1][10]->10005[1][12]->10006[1][14]->10007[1][16]
		keys.add("10008,null");//NONE   [08]10000[1][2]->10001[1][4]->10002[1][6]->10003[1][8]->10004[1][10]->10005[1][12]->10006[1][14]->10007[1][16]->10008[1][18]
		keys.add("10000,null");//NONE   [09]10001[1][4]->10003[1][8]->10002[1][6]->10007[1][16]->10004[1][10]->10005[1][12]->10006[1][14]->10008[1][18]->10000[2][19]
		keys.add("10009,null");//NONE   [10]10001[1][4]->10003[1][8]->10002[1][6]->10007[1][16]->10004[1][10]->10005[1][12]->10006[1][14]->10008[1][18]->10000[2][19]->10009[1][21]
		keys.add("11000,10001");//10001 [11]10002[1][6]->10003[1][8]->10005[1][12]->10007[1][16]->10004[1][10]->10009[1][21]->10006[1][14]->10008[1][18]->10000[2][19]->11000[1][24]
		keys.add("10003,null");//NONE   [12]10002[1][6]->10005[1][12]->10006[1][14]->10007[1][16]->10003[2][25]->10009[1][21]->11000[1][24]->10008[1][18]->10000[2][19]->10004[1][10]
		keys.add("11001,10002");//10002 [13]10004[1][10]->10005[1][12]->10006[1][14]->10007[1][16]->11001[1][28]->10009[1][21]->11000[1][24]->10008[1][18]->10000[2][19]->10003[2][25]
		keys.add("11002,10004");//10004 [14]10005[1][12]->10007[1][16]->10006[1][14]->10008[1][18]->11001[1][28]->10009[1][21]->11000[1][24]->10003[2][25]->10000[2][19]->11002[1][31]
		keys.add("11003,10005");//10005 [15]10006[1][14]->10007[1][16]->10009[1][21]->10008[1][18]->11001[1][28]->11002[1][31]->11000[1][24]->10003[2][25]->10000[2][19]->11003[1][34]
		keys.add("11004,10006");//10006 [16]10007[1][16]->10008[1][18]->10009[1][21]->11003[1][34]->11001[1][28]->11002[1][31]->11000[1][24]->10003[2][25]->10000[2][19]->11004[1][37]
		keys.add("11005,10007");//10007 [17]10008[1][18]->11001[1][28]->10009[1][21]->11003[1][34]->11004[1][37]->11002[1][31]->11000[1][24]->10003[2][25]->10000[2][19]->11005[1][40]
		keys.add("11006,10008");//10008 [18]10009[1][21]->11001[1][28]->11000[1][24]->11003[1][34]->11004[1][37]->11002[1][31]->11005[1][40]->10003[2][25]->10000[2][19]->11006[1][43]
		keys.add("11007,10009");//10009 [19]11000[1][24]->11001[1][28]->11002[1][31]->11003[1][34]->11004[1][37]->11006[1][43]->11005[1][40]->10003[2][25]->10000[2][19]->11007[1][46]
		
		for(String key : keys) {
			keyUsed.set(key);
			cache.get(key.split(",")[0]);
			count.incrementAndGet();
		}
	}
}
