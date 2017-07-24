package com.lhd.mr.friend;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class LastSort extends WritableComparator {

	public LastSort() {
		super(Friend.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Friend friend1 = (Friend)a;
		Friend friend2 = (Friend)b;
		
		int c1 = friend1.getF1().compareTo(friend2.getF1());
		if (c1 == 0) {
			return -Integer.compare(friend1.getCount(), friend2.getCount());
		}
		return c1;
	}

}
