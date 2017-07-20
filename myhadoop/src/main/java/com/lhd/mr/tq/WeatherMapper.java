package com.lhd.mr.tq;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class WeatherMapper extends Mapper<LongWritable, Text, Weather, DoubleWritable > {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] strs = StringUtils.split(value.toString(), '\t');
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		try {
			Date date = sdf.parse(strs[0]);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			
			Weather we = new Weather();
			we.setYear(cal.get(Calendar.YEAR));
			we.setMonth(cal.get(Calendar.MONTH));
			double hot = Double.parseDouble(strs[1].substring(0, strs[1].length() -1));
			we.setHot(hot);
			context.write(we, new DoubleWritable(hot));
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}
