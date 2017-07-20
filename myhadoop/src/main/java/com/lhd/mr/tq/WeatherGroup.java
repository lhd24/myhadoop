package com.lhd.mr.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherGroup extends WritableComparator {

	public WeatherGroup() {
		super(Weather.class, true);
	}
	
	/**
	 * 做分组， 将同一年同一月的数据分为一组
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Weather we1 = (Weather) a;
		Weather we2 = (Weather) b;
		
		int c1 = Integer.compare(we1.getYear(), we2.getYear());
		if (c1 == 0) {
			return Integer.compare(we1.getMonth(), we2.getMonth());
		}
		return c1;
	}

}
