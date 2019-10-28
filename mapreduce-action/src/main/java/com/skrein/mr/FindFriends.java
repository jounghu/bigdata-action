package com.skrein.mr;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FindFriends {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] vals = line.split(":");

            if (vals.length == 2) {
                String person = vals[0];

                String[] follower = vals[1].split(",");

                for (String f : follower) {
                    context.write(new Text(f), new Text(person));
                }
            }
        }
    }


    public static class Combine extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // B (A,E,C)
            List<String> allFollower = new ArrayList<>();
            values.forEach(k -> allFollower.add(k.toString()));
            Collections.sort(allFollower);

            // A C E
            if (!allFollower.isEmpty()) {
                for (int i = 0; i < allFollower.size() - 1; i++) {
                    for (int j = i + 1; j < allFollower.size() - 1; j++) {
                        context.write(new Text(allFollower.get(i) + "-" + allFollower.get(j)), key);
                    }

                }
            }
        }
    }


    public static class Map2 extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] vals = line.split("\t");
            context.write(new Text(vals[0]), new Text(vals[1]));
        }
    }


    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            for (Text val : values) {
                sb.append(val.toString()).append(";");
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(FindFriends.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(Combine.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 2]));
        if (job.waitForCompletion(true)) {
            Job job3 = Job.getInstance(conf, "Average");
            job3.setJarByClass(FindFriends.class);
            job3.setMapperClass(Map2.class);
            job3.setReducerClass(IntSumReducer.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job3, new Path(otherArgs[otherArgs.length - 2]));
            FileOutputFormat.setOutputPath(job3, new Path(otherArgs[otherArgs.length - 1]));
            System.exit(job3.waitForCompletion(true) ? 0 : 1);
        }

    }
}