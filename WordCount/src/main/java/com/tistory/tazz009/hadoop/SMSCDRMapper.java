package com.tistory.tazz009.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SMSCDRMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text status = new Text();
	private final static IntWritable addOne = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split(";");

		if (Integer.parseInt(line[1]) == 1) {
			status.set(line[4]);
			context.write(status, addOne);
		}

	}

}
