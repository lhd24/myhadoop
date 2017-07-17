package com.lhd.hdfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Hello world!
 *
 */
public class Demo 
{
    public static void main( String[] args ) throws IOException, URISyntaxException
    {
    	//加载src目录下的配置文件
    	Configuration config = new Configuration();
    	FileSystem fileSystem =  FileSystem.get(new URI("hdfs://haidong.cloudapp.net:9000"), config);
    	
    	FSDataInputStream fsis = fileSystem.open(new Path("/start-hadoop.sh"));
    	IOUtils.copyBytes(fsis, System.out, 1024, true);
    	IOUtils.closeStream(fsis);
    	
    	FSDataOutputStream fsos = fileSystem.create(new Path("/hello"));
    	FileInputStream fis = new FileInputStream("D:/www.xml");
    	IOUtils.copyBytes(fis, fsos, 1024, true);
    	IOUtils.closeStream(fsos);
    	fileSystem.close();
    }
}
