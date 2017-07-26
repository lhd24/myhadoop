package com.lhd.mr.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {
	
	enum Mycounter{
		my
	}

	public static void main(String[] args) {
		
		double d =0.001;
		Configuration config = new Configuration();
		int i = 0;
		while (true) {
			i++;
			config.setInt("runcount", i);
			try {
				Job job = Job.getInstance(config);
				job.setJarByClass(RunJob.class);
				
				job.setJobName("pageRank_" + i);
				
				job.setMapperClass(PageRankMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setInputFormatClass(KeyValueTextInputFormat.class);
				
				job.setReducerClass(PageRankReducer.class);
				
				Path inputPath = new Path("/usr/pagerank/pagerank.txt");
				if (i > 1) {
					inputPath = new Path("/usr/pagerank/output" + (i-1));
				}
				FileInputFormat.addInputPath(job, inputPath);
				
				Path outpath = new Path("/usr/pagerank/output" + i);
				FileSystem fs = FileSystem.get(config);
				if (fs.exists(outpath)) {
					fs.delete(outpath, true);
				}
				FileOutputFormat.setOutputPath(job, outpath);
				
				boolean iscompleted = job.waitForCompletion(true);
				if (iscompleted) {
					System.out.println("Job successed!");
					long sum= job.getCounters().findCounter(Mycounter.my).getValue();
					System.out.println(sum);
					double avgd= sum/4000.0;
					if(avgd<d){
						System.out.println("All Job successed!");
						break;
					}
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	static class PageRankMapper extends Mapper<Text, Text, Text, Text>{

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			Node node = null;
			int runcount = context.getConfiguration().getInt("runcount", 1);
			if (runcount == 1) {
				node = Node.fromMR("1.0" + "\t" + value.toString());
			}else{
				node = Node.fromMR(value.toString());
			}
			context.write(key, new Text(node.toString()));
			
			if (node.containsAdjacentNodes()) {
				String[] nodes = node.getAdjacentNodeNames();
				double outValue = node.getPageRank()/nodes.length;
				for (int i = 0; i < nodes.length; i++) {
					context.write(new Text(nodes[i]), new Text(outValue + ""));
				}
			}
		}
	}
	
	static class PageRankReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text text, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			Node sourcenode = null;
			for (Text i : iterable) {
				Node node = Node.fromMR(i.toString());
				if (node.containsAdjacentNodes()) {
					sourcenode = node;
				}else{
					sum = sum + node.getPageRank();
				}
			}
			
			double newPageRank = (1-0.85)/4 + 0.85*(sum);
			System.out.println("*********** new pageRank value is "+newPageRank);
			
			//把新的pr值和计算之前的pr比较
			double d= sourcenode.getPageRank() - newPageRank;
			int j= (int)(d*1000.0);
			j= Math.abs(j);
			System.out.println(j+"___________");
			//reducer的计数器
			context.getCounter(Mycounter.my).increment(j);;
			
			sourcenode.setPageRank(newPageRank);
			context.write(text, new Text(sourcenode.toString()));
		}
	}
}
