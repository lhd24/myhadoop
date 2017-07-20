package com.lhd.mr.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherSort extends WritableComparator {

	public WeatherSort(){
		super(Weather.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Weather we1 = (Weather) a;
		Weather we2 = (Weather) b;
		
		int c1 = Integer.compare(we1.getYear(), we2.getYear());
		if (c1 == 0) {
			int c2 = Integer.compare(we1.getMonth(), we2.getMonth());
			if (c2 == 0) {
				return -Double.compare(we1.getHot(), we2.getHot());
			}
			return c2;
		}
		return c1;
	}
}
