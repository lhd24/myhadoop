package com.lhd.mr.tfidf;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

/**
 * 第一个MR，计算TF和计算N(微博总数)
 * @author root
 *
 */
public class TfMapper extends Mapper<Text, Text, Text, IntWritable > {

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		StringReader sr=new StringReader(value.toString());  
        IKSegmenter ik=new IKSegmenter(sr, true);  
        Lexeme lex=null;  
        while((lex=ik.next())!=null){  
        	context.write(new Text(key + "_" + lex.getLexemeText()), new IntWritable(1));
        }  
        context.write(new Text("count"), new IntWritable(1));
	}
}
