package com.bfm.app.timeseries;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;

public class TimeSeriesFieldTypeSuppliers {
	
	public static final Supplier<TIntArrayList> INTEGER_TYPE_SUPPLIER = TIntArrayList::new;
	public static final Supplier<TLongArrayList> LONG_TYPE_SUPPLIER = TLongArrayList::new;
	public static final Supplier<TDoubleArrayList> DOUBLE_TYPE_SUPPLIER = TDoubleArrayList::new;
	public static final Supplier<List<String>> STRING_TYPE_SUPPLIER = () -> new ArrayList<String>();
}
