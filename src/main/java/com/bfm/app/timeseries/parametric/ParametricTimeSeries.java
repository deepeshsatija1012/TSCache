package com.bfm.app.timeseries.parametric;

import com.bfm.app.timeseries.TimeSeriesEntry;

import gnu.trove.list.array.TDoubleArrayList;

public class ParametricTimeSeries extends TimeSeriesEntry {
	
	public void setFactor(String factor) {
		super.setKey(factor);
	}
	
	public String getFactor() {
		return super.getKey();
	}
	
	public void addDelta(double delta) {
		super.getDoubleField("deltas").add(delta);
	}
	
	public void addValue(double value) {
		super.getDoubleField("values").add(value);
	}
	
	public void setDeltas(TDoubleArrayList deltas) {
		super.getDoubleField("deltas").addAll(deltas);
	}
	
	public void setValues(TDoubleArrayList values) {
		super.getDoubleField("values").addAll(values);
	}
	
	public TDoubleArrayList getDeltas() {
		return super.getDoubleField("deltas");
	}
	
	public TDoubleArrayList getValues() {
		return super.getDoubleField("values");
	}

}
