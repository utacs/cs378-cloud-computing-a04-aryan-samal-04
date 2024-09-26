package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, Text, Text, FloatWritable> {

   public void reduce(Text text, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {
	   
        float finalAmount = 0;
        float finalTripTime = 0;
        
        for (Text value : values) {
            String[] tmp = value.toString().split(",");
            if (tmp.length == 2) { 
                try {
                    finalAmount += Float.parseFloat(tmp[0]);
                    finalTripTime += Float.parseFloat(tmp[1]);
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing value: " + value.toString());
                }
            }
        }

        if (finalTripTime != 0) {
            context.write(text, new FloatWritable(finalAmount / (finalTripTime / 60)));
        }
   }
}