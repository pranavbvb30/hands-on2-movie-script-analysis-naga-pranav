package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line into character name and dialogue based on colon ':'
        String line = value.toString();
        String[] parts = line.split(":", 2); // Split into character and dialogue

        if (parts.length == 2) {
            String character = parts[0].trim(); // Character name
            String dialogue = parts[1].trim();  // Dialogue

            // Tokenize the dialogue into words
            StringTokenizer tokenizer = new StringTokenizer(dialogue);

            // For each word in the dialogue, emit (character:word, 1)
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z]", ""); // Clean word
                if (!word.isEmpty()) {
                    characterWord.set(character + ":" + word);
                    context.write(characterWord, one); // Emit (character:word, 1)
                }
            }
        }
    }
}
