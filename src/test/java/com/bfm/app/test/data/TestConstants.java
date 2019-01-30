package com.bfm.app.test.data;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

import com.bfm.app.timeseries.classifiers.IntervalType;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestConstants {
	
	
	public static List<LocalDateTime> getHolidayCalender(String cal){
		List<LocalDateTime> list = Lists.newArrayList();
		List<String> dates = Collections.emptyList();
		try {
			dates = Files.readLines(new File("Y:/slash/usr/local/bfm/calendars/"+cal), Charset.forName("UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(String date : dates) {
			String year = date.substring(0, 4), month = date.substring(4, 6), day = date.substring(6);
			LocalDateTime dt = LocalDate.of(Integer.parseInt(year), Integer.parseInt(month), Integer.parseInt(day)).atStartOfDay(ZoneId.systemDefault()).toLocalDateTime();
			list.add(dt);
		}
		return list;
	}
	
	private static final DateTimeFormatter FROMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
	public static final IntervalType IST = new IntervalType(getHolidayCalender("IN"), null) {

		@Override
		public LocalDateTime getNext(LocalDateTime localDate) {
			return localDate.plusDays(1);
		}
		@Override
		public String format(long time) {
			return FROMAT.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault()));
		}
	};
}
