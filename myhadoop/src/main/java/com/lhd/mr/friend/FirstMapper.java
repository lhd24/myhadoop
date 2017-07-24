package com.lhd.mr.friend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] name = StringUtils.split(value.toString(), '\t');
		for (int i = 0; i < name.length; i++) {
			context.write(new Text(name[0] +"-" + name[i]), new IntWritable(0));
			for (int j = i +1; j < name.length; j++) {
				context.write(new Text(getUnionName(name[i], name[j])), new IntWritable(1));
			}
		}
	}
	
	private String getUnionName(String n1, String n2){
		int c = n1.compareTo(n2);
		if (c < 0) {
			return n2 + "-" + n1;
		}
		return n1 + "-" + n2;
	}
	
}
