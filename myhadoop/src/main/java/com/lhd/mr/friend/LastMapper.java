package com.lhd.mr.friend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class LastMapper extends Mapper<Text, Text, Friend, IntWritable> {

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] strs = StringUtils.split(key.toString(), '-');
		System.out.println(key.toString());
		Friend f1 = new Friend();
		f1.setF1(strs[0]);
		f1.setF2(strs[1]);
		f1.setCount(Integer.parseInt(value.toString()));
		
		Friend f2 = new Friend();
		f2.setF1(strs[1]);
		f2.setF2(strs[0]);
		f2.setCount(Integer.parseInt(value.toString()));
		
		context.write(f1, new IntWritable(Integer.parseInt(value.toString())));
		context.write(f2, new IntWritable(Integer.parseInt(value.toString())));
	}
}
