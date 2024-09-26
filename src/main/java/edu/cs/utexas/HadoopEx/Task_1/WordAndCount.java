package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class WordAndCount implements Comparable<WordAndCount> {

    // word to define taxiId and count to define number of errors
    private final Text word;
    private final IntWritable count;

    // constructor
    public WordAndCount(Text word, IntWritable count) {
        this.word = word;
        this.count = count;
    }

    // getters and setters
    public Text getWord() {
        return word;
    }

    public IntWritable getCount() {
        return count;
    }

    // compare function
    @Override
    public int compareTo(WordAndCount other) {
        float diff = Integer.parseInt(word.toString()) - Integer.parseInt(other.word.toString());
        if (diff > 0) {
            return 1;
        } else if (diff < 0) {
            return -1;
        }
        return 0;
    }

    // to string for printing
    public String toString(){
        return "("+word.toString() +", "+ count.toString()+")";
    }
}

