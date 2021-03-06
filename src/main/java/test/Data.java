package test;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;

import com.bfm.app.timeseries.TimeSeriesEntryBuilder;
import com.bfm.app.timeseries.parametric.ParametricTimeSeries;

import gnu.trove.list.array.TDoubleArrayList;

public class Data {
	private static final double[] DOUBLE_DATA = new double[2520];
	
	private static final Map<String, ParametricTimeSeries> DATA = new HashMap<>();
	
	public Data() {
		Random r  = new Random();
		LocalDate today = LocalDate.now();
		for(int i=0;i<2520;i++) {
			DOUBLE_DATA[i] = r.nextDouble();
			if(today.getDayOfWeek()!=DayOfWeek.SATURDAY && today.getDayOfWeek()!=DayOfWeek.SUNDAY) {
				today = today.plusDays(-1);
			}
		}
		Long time = today.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
		double[] levels = new double[2520];
		Arrays.fill(levels, -0.0d);
		long end = LocalDate.now().atStartOfDay().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
		TDoubleArrayList levelList = TDoubleArrayList.wrap(levels);
		for(int i=10000;i<20000;i++) {
//			TimeSeriesEntryBuilder<ParametricTimeSeries> builder = new TimeSeriesEntryBuilder<>(i+"", ParametricTimeSeries::new);
			TimeSeriesEntryBuilder<ParametricTimeSeries> builder = new TimeSeriesEntryBuilder<>();
			builder.key(i+"");
			builder.objectBuilder(ParametricTimeSeries::new);
			builder.addField("deltas", Double.class);
			builder.addField("values", Double.class);
			builder.startTime(time);builder.endTime(end);
			builder.storedStartTime(time); builder.storedEndTime(end);
			TDoubleArrayList list = TDoubleArrayList.wrap(ArrayUtils.clone(DOUBLE_DATA));
			ParametricTimeSeries ts = builder.build();
			ts.setDeltas(list); ts.setValues(levelList);
			ts.setFactor(i+"");
			
			DATA.put(ts.getFactor(), ts);
		}
	}
	
	public ParametricTimeSeries getTimeSeries(String factor) {
		return DATA.get(factor);
	}
	
	public Map<String, ParametricTimeSeries> getTimeSeries(Set<String> factors) {
		return factors.stream().map(fac -> DATA.get(fac)).collect(Collectors.toMap(ParametricTimeSeries::getFactor, d -> d));
	}
	
	public Map<String, ParametricTimeSeries> getAll(){
		return DATA;
	}
	
	

}
