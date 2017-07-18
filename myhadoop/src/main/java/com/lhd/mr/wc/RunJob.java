package com.lhd.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {

	public static void main(String[] args){
		Configuration config = new Configuration();
		
		try {
			Job job = Job.getInstance(config);
			job.setJarByClass(RunJob.class);
			
			job.setJobName("wc");
			
			job.setMapperClass(WordCountMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setReducerClass(WordCountReducer.class);
			
			//这里有一个坑，可能会错误引用到 org.apache.hadoop.mapred.FileInputFormat 包，它需要Jobconf对象
			//所有在 org.apache.hadoop.mapred 下面的包 都是旧的API，新的API 在 org.apache.hadoop.mapreduce 包下
			//所以引用的时候要特别当心，不要引错
			//https://stackoverflow.com/questions/18402360/what-is-the-basic-difference-between-jobconf-and-job
			FileInputFormat.addInputPath(job, new Path("/usr/input/"));
			
			Path outpath = new Path("/usr/output/wc");
			FileSystem fs = FileSystem.get(config);
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);
			
			boolean iscompleted = job.waitForCompletion(true);
			if (iscompleted) {
				System.out.println("Job任务执行成功");
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}