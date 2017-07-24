package com.lhd.mr.friend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LastReducer extends Reducer<Friend, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Friend friend, Iterable<IntWritable> iterable, Context context) 
			throws IOException, InterruptedException {
		context.write(new Text(friend.getF1() + "-" + friend.getF2()), 
				new IntWritable(friend.getCount()));
	}

}
