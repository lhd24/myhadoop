package com.lhd.mr.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherJob {
	public static void main(String[] args){
		Configuration config = new Configuration();
		
		try {
			Job job = Job.getInstance(config);
			job.setJarByClass(WeatherJob.class);
			
			job.setJobName("weather");
			
			job.setMapperClass(WeatherMapper.class);
			job.setMapOutputKeyClass(Weather.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			
			job.setReducerClass(WeatherReducer.class);
			
			//设置分区，排序，分组
			job.setPartitionerClass(WeatherPartition.class);
			job.setSortComparatorClass(WeatherGroup.class);
			job.setGroupingComparatorClass(WeatherGroup.class);
			
			job.setNumReduceTasks(3);
			
			FileInputFormat.addInputPath(job, new Path("/usr/tq/"));
			
			Path outpath = new Path("/usr/tq/output");
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
