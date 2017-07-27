package com.lhd.mr.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class TfPartition extends HashPartitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		if (key.toString().equals("count")) {
			return 3;
		}
		return super.getPartition(key, value, numReduceTasks -1);
	}

}
