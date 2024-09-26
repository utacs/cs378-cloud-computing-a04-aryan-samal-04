package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;


public class WordAndCount implements Comparable<WordAndCount> {

    // get id and count - error fraction now instead of total number of errors
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

    // compare function logic for error fractions
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

    // to string for print
    public String toString(){
        return "("+word.toString() +", "+ count.toString()+")";
    }
}