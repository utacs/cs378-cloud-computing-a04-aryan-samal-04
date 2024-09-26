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
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		try {
			if (fields.length == 17 && (Float.parseFloat(fields[6]) == 0 || Float.parseFloat(fields[7]) == 0 || 
				fields[6].isEmpty() || fields[7].isEmpty())) {
				String pickupDate = fields[2].substring(11, 13);
				int pickupInt = Integer.parseInt(pickupDate);
				if (pickupInt == 0) {
					pickupInt = 24;
				}
			
				word.set(Integer.toString(pickupInt));
				context.write(word, counter);
			}
			if (fields.length == 17 && (Float.parseFloat(fields[8]) == 0 || Float.parseFloat(fields[9]) == 0 ||
				fields[8].isEmpty() || fields[9].isEmpty())) {
				String dropoffDate = fields[3].substring(11, 13);
				int dropoffDateInt = Integer.parseInt(dropoffDate);
				if (dropoffDateInt == 0) {
					dropoffDateInt = 24;
				}
				
			
				word.set(Integer.toString(dropoffDateInt));
				context.write(word, counter);
			}
		} catch (NumberFormatException e) {
			System.out.println();
		}
	}
}