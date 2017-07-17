package com.lhd.hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class HdfsDemo {

	FileSystem fileSystem;
	Configuration config;
	
	@Before
	public void begin() throws Exception{
		//加载src目录下的配置文件
    	config = new Configuration();
    	fileSystem =  FileSystem.get(config);
	}
	
	@After
	public void end(){
		try{
			fileSystem.close();
		} catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		}
	}
	
	@Test
	public void mkdir() throws IOException{
		Path path = new Path("/tmp");
		fileSystem.mkdirs(path);
	}
	
	@Test 
	public void upload() throws IOException{
		Path path = new Path("/tmp/test");
		FSDataOutputStream outputStream = fileSystem.create(path);
		String relativelyPath = System.getProperty("user.dir"); 
		FileUtils.copyFile(new File(relativelyPath + "/data/tmp/test"), outputStream);
	}
	
	@Test
	public void list() throws FileNotFoundException, IOException{
		Path path = new Path("/tmp");
		FileStatus[] fss = fileSystem.listStatus(path);
		for(FileStatus fs : fss){
			System.out.println(fs.getPath() + "-" + fs.getLen() + "-" + fs.getAccessTime());
		}
	}
	
	@Test
	public void uploadSmall() throws Exception{
		Path path = new Path("/tmp/seq");		
		SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, config, path, Text.class, Text.class);
		String relativelyPath=System.getProperty("user.dir"); 
		File file = new File(relativelyPath + "/data/tmp");
		for(File f : file.listFiles()){
			writer.append(new Text(f.getName()), new Text(FileUtils.readFileToString(f)));
		}
	}
	
	@Test
	public void downloadSmallFile() throws IOException {
		Path path = new Path("/tmp/seq");	
		SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path, config);
		Text key = new Text();
		Text value = new Text();
		while(reader.next(key, value)){
			System.out.println(key);
			System.out.println(value);
			System.out.println("--------------");
		}
	}
	
}
