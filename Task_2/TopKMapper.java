package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class TopKMapper extends Mapper<Text, Text, Text, FloatWritable> {

	// setting up pq
	private PriorityQueue<WordAndCount> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();
	}

	// filters topK results
	public void map(Text key, FloatWritable value, Context context)
			throws IOException, InterruptedException {
		// get the error fraction from previous reducer
        float errorFraction = value.get();

		// add tuple to pq and limit it to size of 5
        pq.add(new WordAndCount(new Text(key), new FloatWritable(errorFraction)));
        if (pq.size() > 5) {
            pq.poll();
        }
	}

	// function to cleanup and print the top 5 results of the pq by polling
	public void cleanup(Context context) throws IOException, InterruptedException {
		while (pq.size() > 0) {
			WordAndCount wordAndCount = pq.poll();
			context.write(wordAndCount.getWord(), wordAndCount.getCount());
		}
	}

}