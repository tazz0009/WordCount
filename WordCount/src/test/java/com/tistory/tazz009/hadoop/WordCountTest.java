package com.tistory.tazz009.hadoop;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountTest {

	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver;
	MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		WordCountMapper mapper = new WordCountMapper();
		WordCountReducer reducer = new WordCountReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testMap() throws IOException {
	    Text value = new Text("Hello World Bye World");
	    mapDriver.withInput(new LongWritable(), value);
	    mapDriver.withOutput(new Text("Hello"), new IntWritable(1));
	    mapDriver.withOutput(new Text("World"), new IntWritable(1));
	    mapDriver.withOutput(new Text("Bye"), new IntWritable(1));
	    mapDriver.withOutput(new Text("World"), new IntWritable(1));
		mapDriver.runTest();
	}

}
