package com.bfm.app.timeseries.parametric;

import java.util.function.Supplier;

public class ParametricTimeSeriesObjectSupplier implements Supplier<ParametricTimeSeries> {

	@Override
	public ParametricTimeSeries get() {
		return new ParametricTimeSeries();
	}

}
