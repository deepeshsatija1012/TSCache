package com.bfm.app.test.timeseries;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import com.bfm.app.test.data.TestConstants;
import com.bfm.app.timeseries.TimeSeriesEntry;
import com.bfm.app.timeseries.TimeSeriesEntryBuilder;
import com.bfm.app.timeseries.classifiers.IntervalType;
import com.bfm.app.timeseries.parametric.ParametricTimeSeries;

import gnu.trove.list.array.TDoubleArrayList;


public class TimeSeriesEntryTest {
	
	private static final ParametricTimeSeries getData() {
		double[] DOUBLE_DATA = new double[2520];
		Random r = new Random();
		LocalDate today = LocalDate.of(2019, 1, 28);
		for (int i = 0; i < 2520;) {
			if (today.getDayOfWeek() != DayOfWeek.SATURDAY && today.getDayOfWeek() != DayOfWeek.SUNDAY) {
				DOUBLE_DATA[i++] = r.nextDouble();
			}
			today = today.plusDays(-1);
		}
		Long time = today.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
		double[] levels = new double[2520];
		Arrays.fill(levels, -0.0d);
		TDoubleArrayList levelList = TDoubleArrayList.wrap(levels);
		TimeSeriesEntryBuilder<ParametricTimeSeries> builder = new TimeSeriesEntryBuilder<>();
		builder.key("ABC");
		builder.interval(IntervalType.DAILY);
		builder.objectBuilder(ParametricTimeSeries::new);
		builder.addField("deltas", Double.class);
		builder.addField("values", Double.class);
		builder.startTime(today.atStartOfDay());
		builder.endTime(LocalDate.of(2019, 1, 28).atStartOfDay());
		builder.storedStartTime(time);
		builder.storedEndTime(LocalDate.of(2019, 1, 28).atStartOfDay().atZone(ZoneId.systemDefault()).toLocalDateTime());
		TDoubleArrayList list = TDoubleArrayList.wrap(ArrayUtils.clone(DOUBLE_DATA));
		ParametricTimeSeries ts = builder.build();
		ts.setDeltas(list);
		ts.setValues(levelList);
		ts.setFactor("ABC");
		return ts;
	}
	
	@Test
	public void test() {
		ParametricTimeSeries ts = getData();
		TDoubleArrayList ll = new TDoubleArrayList();
		for(int i=2240;i<2262;i++) {
			if(i==2259) continue; //skipping 26th Jan
			ll.add(ts.getDeltas().get(i));
		}
		LocalDateTime start = LocalDateTime.of(2018, 1, 1, 0, 0).atZone(ZoneId.systemDefault()).toLocalDateTime();
		LocalDateTime end = LocalDateTime.of(2018, 1, 31, 0, 0).atZone(ZoneId.systemDefault()).toLocalDateTime();
		TimeSeriesEntry sampledEntry = ts.sample(start, end, TestConstants.IST);
		double[] actual = sampledEntry.getDoubleField("deltas").toArray();
		Assert.assertTrue(Arrays.equals(ll.toArray(), actual));
	}

}
