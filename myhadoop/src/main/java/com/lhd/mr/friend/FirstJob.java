package com.lhd.mr.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FirstJob {

	public static void main(String[] args) {
		Configuration config = new Configuration();
		
		try {
			Job job = Job.getInstance(config);
			job.setJarByClass(FirstJob.class);
			
			job.setJobName("fof1");
			
			job.setMapperClass(FirstMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setReducerClass(FirstReducer.class);
			
			FileInputFormat.addInputPath(job, new Path("/usr/friend/"));
			
			Path outpath = new Path("/usr/friend/output");
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