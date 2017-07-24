package com.lhd.mr.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LastJob {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		
		try {
			Job job = Job.getInstance(config);
			job.setJarByClass(LastJob.class);
			
			job.setJobName("fof2");
			
			job.setMapperClass(LastMapper.class);
			job.setMapOutputKeyClass(Friend.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setReducerClass(LastReducer.class);
			
			//TextInputFormat为默认解析类
			//TextInputFormat把数据源中的数据解析成一行行记录，每一行记录对应一个键值对。所以类型是定死的
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			//每一个键值对会被Map函数调用一次
			
			FileInputFormat.addInputPath(job, new Path("/usr/friend/output"));
			
			Path outpath = new Path("/usr/friend/output2");
			FileSystem fs = FileSystem.get(config);
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);
			
			boolean iscompleted = job.waitForCompletion(true);
			if (iscompleted) {
				System.out.println("Job successed!");
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}