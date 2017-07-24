package com.lhd.mr.friend;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class LastGroup extends WritableComparator {

	public LastGroup() {
		super(Friend.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Friend friend1 = (Friend)a;
		Friend friend2 = (Friend)b;
		
		int c1 = friend1.getF1().compareTo(friend2.getF1());
		if (c1 == 0) {
			int c2 = friend1.getF2().compareTo(friend2.getF2());
			return c2;
		}
		return c1;
	}
	
}
