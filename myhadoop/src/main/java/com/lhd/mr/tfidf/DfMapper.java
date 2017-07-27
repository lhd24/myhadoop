package com.lhd.mr.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

//统计df：词在多少个微博中出现过。
public class DfMapper extends Mapper<Text, Text, Text, IntWritable> {

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		//获取当前 mapper task的数据片段(split)
		FileSplit fs = (FileSplit)context.getInputSplit();
		if (!fs.getPath().getName().contains("part-r-00003")) {
			String ss[] = StringUtils.split(key.toString(), '_');
			if (ss.length > 1) {
				context.write(new Text(ss[1]), new IntWritable(1));
			}
		}
	}
}
