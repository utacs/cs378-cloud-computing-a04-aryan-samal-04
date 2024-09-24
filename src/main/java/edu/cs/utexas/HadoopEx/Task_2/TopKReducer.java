package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Iterator;



public class TopKReducer extends  Reducer<Text, Text, Text, FloatWritable> {

    private PriorityQueue<WordAndCount> pq = new PriorityQueue<WordAndCount>(5);;


//    public void setup(Context context) {
//
//        pq = new PriorityQueue<WordAndCount>(10);
//    }


    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * @param text
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
   public void reduce(Text key, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {

       // size of values is 1 because key only has one distinct value
       for (Text value : values) {
            float errorFraction = Float.parseFloat(value.toString());
            pq.add(new WordAndCount(new Text(key), new FloatWritable(errorFraction)));
        }

       // keep the priorityQueue size <= heapSize
       while (pq.size() > 5) {
           pq.poll();
       }


   }


    public void cleanup(Context context) throws IOException, InterruptedException {
        List<WordAndCount> values = new ArrayList<WordAndCount>(5);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }


        // reverse so they are ordered in descending order
        Collections.reverse(values);


        for (WordAndCount value : values) {
            context.write(value.getWord(), value.getCount());
        }


    }

}