package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CharacterWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int wordCount = 0;

        // Sum all the word counts for this character
        for (IntWritable value : values) {
            wordCount += value.get();
        }

        // Write the character and their total word count to the output
        context.write(key, new IntWritable(wordCount));
    }
}
