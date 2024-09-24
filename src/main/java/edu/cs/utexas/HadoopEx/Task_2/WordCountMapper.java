package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text taxiId = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		if (fields.length == 17) {
			try {
				taxiId.set(fields[0]);
				boolean isError = Float.parseFloat(fields[6]) == 0 || Float.parseFloat(fields[7]) == 0 || 
				Float.parseFloat(fields[8]) == 0 || Float.parseFloat(fields[6]) == 0 || fields[6].isEmpty()
				|| fields[7].isEmpty() || fields[8].isEmpty() || fields[9].isEmpty();

				if (isError) {
                	context.write(taxiId, new IntWritable(1));  // Error record
				} else {
					context.write(taxiId, new IntWritable(0));  // Valid record
				}
			} catch (NumberFormatException e) {
				System.out.println();
			}
		}
	}
}