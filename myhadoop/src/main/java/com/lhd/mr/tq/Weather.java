package com.lhd.mr.tq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Weather implements WritableComparable<Weather> {

	private int year;
	private int month;
	private double hot;
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public double getHot() {
		return hot;
	}

	public void setHot(double hot) {
		this.hot = hot;
	}
	
	/**
	 * 序列化
	 */
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeDouble(hot);
	}

	/**
	 * 反序列化
	 */
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.hot = in.readDouble();
	}

	/**
	 * 比较 （系统默认调用。比较key是否相同）
	 */
	public int compareTo(Weather tq) {
		int c1 = Integer.compare(this.year, tq.getYear());
		if (c1 == 0) {
			int c2 = Integer.compare(this.month, tq.getMonth());
			if (c2 == 0) {
				return Double.compare(this.hot, tq.getHot());
			}
			return c2;
		}
		return c1;
	}

}
