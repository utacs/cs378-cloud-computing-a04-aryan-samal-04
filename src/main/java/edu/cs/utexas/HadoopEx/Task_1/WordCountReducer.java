package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Iterator;

public class WordCountReducer extends  Reducer<Text, IntWritable, Text, NullWritable> {

    // setting up pq based on error counts
    private PriorityQueue<WordAndCount> pq = new PriorityQueue<WordAndCount>(24);;


    public void reduce(Text text, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        
        // receive total error sum
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        
        //context.write(text, new IntWritable(sum));
        pq.add(new WordAndCount(new Text(text), new IntWritable(sum)));
    }

    // function to cleanup and print the 24 hours (1-24) and report number of errors
    public void cleanup(Context context) throws IOException, InterruptedException {
        List<WordAndCount> values = new ArrayList<WordAndCount>(24);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        for (WordAndCount value : values) {
            context.write(new Text(value.toString()), NullWritable.get());
        }
    }
}