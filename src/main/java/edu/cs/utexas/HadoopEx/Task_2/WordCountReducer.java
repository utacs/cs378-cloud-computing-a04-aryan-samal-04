package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, IntWritable, Text, FloatWritable> {

   public void reduce(Text text, Iterable<IntWritable> values, Context context)
           throws IOException, InterruptedException {
	   
        int totalCount = 0;
        int errorCount = 0;

        for (IntWritable value : values) {
            totalCount++;
            if (value.get() == 1) {
                errorCount++;
            }
        }

        float errorFraction = (float) errorCount / totalCount;
        context.write(text, new FloatWritable(errorFraction));
   }
}