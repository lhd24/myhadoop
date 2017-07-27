package com.lhd.mr.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * tf-idf：词频-逆向文档频率
 * @author root
 *
 */
public class TfJob {

	public static void main(String[] args) {
		Configuration config = new Configuration();
		
		try {
			Job job = Job.getInstance(config);
			job.setJarByClass(TfJob.class);
			
			job.setJobName("TfJob");
			
			job.setMapperClass(TfMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setReducerClass(TfReducer.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			//设置分区
			job.setPartitionerClass(TfPartition.class);
			job.setNumReduceTasks(4);
			//设置combiner
			job.setCombinerClass(TfReducer.class);
			
			FileInputFormat.addInputPath(job, new Path("/usr/tfidf/"));
			
			Path outpath = new Path("/usr/tfidf/output");
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
			e.printStackTrace();
		}

	}
}
