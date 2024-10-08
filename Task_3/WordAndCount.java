package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;


public class WordAndCount implements Comparable<WordAndCount> {
    // word - driverId, count - earnings/min
    private final Text word;
    private final FloatWritable count;

    // constructor
    public WordAndCount(Text word, FloatWritable count) {
        this.word = word;
        this.count = count;
    }

    // getters and setters
    public Text getWord() {
        return word;
    }

    public FloatWritable getCount() {
        return count;
    }

    // compare to function for the earnings/min ratio
    @Override
    public int compareTo(WordAndCount other) {
        float diff = count.get() - other.count.get();
        if (diff > 0) {
            return 1;
        } else if (diff < 0) {
            return -1;
        }
        return 0;
    }

    // function to print strings
    public String toString(){
        return "("+word.toString() +", "+ count.toString()+")";
    }
}