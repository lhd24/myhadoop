package com.lhd.mr.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TfIdfJob {

	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("mapred.jar", "C:\\Users\\haidong\\Desktop\\TfIdfJob.jar");
		try {
			Job job =Job.getInstance(config);
			
			//DistributedCache.addCacheFile(uri, conf);
			//2.5
			//把微博总数加载到内存
			job.addCacheFile(new Path("/usr/tfidf/output/part-r-00003").toUri());
			//把df加载到内存
			job.addCacheFile(new Path("/usr/tfidf/dfoutput/part-r-00000").toUri());
			
			job.setJarByClass(TfIdfJob.class);
			
			job.setJobName("TfIdfJob");
			
			job.setMapperClass(TfIdfMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setReducerClass(TfIdfReducer.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path("/usr/tfidf/output/"));
			
			Path outpath = new Path("/usr/tfidf/tfidf");
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
