package com.lhd.mr.tq;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

//注意引包， 不要引 org.apache.hadoop.mapred.lib.HashPartitioner;
public class WeatherPartition extends HashPartitioner<Weather, DoubleWritable> {

	@Override
	public int getPartition(Weather key, DoubleWritable value, int numReduceTasks) {
		// 原则越简单越好
		return key.getYear() % numReduceTasks;
	}
}
