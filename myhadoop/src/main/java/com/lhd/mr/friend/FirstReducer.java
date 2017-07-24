package com.lhd.mr.friend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable, Context context)
			throws IOException, InterruptedException {
		boolean flag = true;
		int sum = 0;
		for (IntWritable i : iterable) {
			sum += i.get();
			if (i.get() == 0) {
				flag = false;
			}
		}
		if (flag) {
			context.write(text, new IntWritable(sum));
		}
	}

}
