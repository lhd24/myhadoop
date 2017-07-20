package com.lhd.mr.tq;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Weather, DoubleWritable, Text, NullWritable> {

	@Override
	protected void reduce(Weather we, Iterable<DoubleWritable> iterable, Context cxt)
			throws IOException, InterruptedException {
		int i = 0;
		
		for(DoubleWritable d: iterable){
			if (i == 2) break;
			
			String msg = we.getYear() + "-" + we.getMonth() +"-" + d + "c";
			cxt.write(new Text(msg), NullWritable.get());
			i++;
		}
		
	}

}
