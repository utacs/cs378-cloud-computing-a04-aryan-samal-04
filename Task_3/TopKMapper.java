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

	// receive results from last reducer and map topk earnings/minute
	public void map(Text key, FloatWritable value, Context context)
			throws IOException, InterruptedException {
		// retrieve value
        float moneyPerMin = value.get();

		// add in to pq
        pq.add(new WordAndCount(new Text(key), new FloatWritable(moneyPerMin)));

		// keep only the top 10
        if (pq.size() > 10) {
            pq.poll();
        }
	}

	// cleanup function to report the results from pq
	public void cleanup(Context context) throws IOException, InterruptedException {
		while (pq.size() > 0) {
			WordAndCount wordAndCount = pq.poll();
			context.write(wordAndCount.getWord(), wordAndCount.getCount());
		}
	}

}