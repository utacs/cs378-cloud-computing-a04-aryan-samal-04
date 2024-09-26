package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, Text> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text driverId = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		if (fields.length == 17) {
			try {
				driverId.set(fields[1]);
				float tripTimeSec = Float.parseFloat(fields[4]);
				float totalAmount = Float.parseFloat(fields[16]);

				context.write(driverId, new Text(totalAmount + ", " + tripTimeSec));
			} catch (NumberFormatException e) {
				System.out.println("erorr line");
			}
		}
	}
}