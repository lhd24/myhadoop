package com.lhd.mr.friend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Friend implements WritableComparable<Friend> {

	private String f1;
	private String f2;
	private int count;
	
	public String getF1() {
		return f1;
	}
	public void setF1(String f1) {
		this.f1 = f1;
	}
	public String getF2() {
		return f2;
	}
	public void setF2(String f2) {
		this.f2 = f2;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(f1);
		out.writeUTF(f2);
		out.writeInt(count);
	}
	public void readFields(DataInput in) throws IOException {
		f1 = in.readUTF();
		f2 = in.readUTF();
		count = in.readInt();
	}
	public int compareTo(Friend o) {
		int c1 = this.f1.compareTo(o.getF1());
		if (c1 == 0) {
			int c2 = this.f2.compareTo(o.getF2());
			if (c2 == 0) {
				return Integer.compare(this.count, o.getCount());
			}			
			return c2;
		}
		return c1;
	}
}
