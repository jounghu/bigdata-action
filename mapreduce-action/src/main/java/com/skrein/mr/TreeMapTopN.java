package com.skrein.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.util.ajax.JSON;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * @author :hujiansong
 * @date :2019/10/24 10:38
 * @since :1.8
 */
public class TreeMapTopN {
    public static class TokenizerMapper
            extends Mapper<Object, Text, NullWritable, Phone> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] vals = line.split(" ");
            context.write(NullWritable.get(), new Phone(vals[0], vals[1]));
        }
    }

    public static class Phone implements Writable, Comparable<Phone> {
        private String phone;

        private String flowNum;

        public Phone(String phone, String flowNum) {
            this.phone = phone;
            this.flowNum = flowNum;
        }

        public Phone() {
        }

        public String getPhone() {
            return phone;
        }

        public String getFlowNum() {
            return flowNum;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(phone);
            out.writeUTF(flowNum);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.phone = in.readUTF();
            this.flowNum = in.readUTF();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Phone phone1 = (Phone) o;
            return phone.equals(phone1.phone) &&
                    flowNum.equals(phone1.flowNum);
        }

        @Override
        public String toString() {
            return phone + " " + flowNum;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getPhone(), getFlowNum());
        }

        @Override
        public int compareTo(Phone o) {
            return -1 * Long.compare(Long.parseLong(this.flowNum), Long.parseLong(o.getFlowNum()));
        }
    }


    public static class IntSumReducer
            extends Reducer<NullWritable, Phone, NullWritable, Phone> {
        private static final Log logger = LogFactory.getLog(IntSumReducer.class);

        private TreeMap<Phone, Long> treeMap = new TreeMap<>();

        private int N = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // default top5
            N = context.getConfiguration().getInt("N", 5);
        }

        public void reduce(NullWritable key, Iterable<Phone> values,
                           Context context
        ) throws IOException, InterruptedException {

                logger.error("values: "+ JSON.toString(values));
            for (Phone value : values) {
                // 注意这里 value的hashcode都是一样的
                Phone copyPhone = new Phone(value.getPhone(),value.getFlowNum());
                treeMap.put(copyPhone, Long.parseLong(copyPhone.getFlowNum()));
                logger.error("treeMapSize=" + treeMap.size() + " N=" + N);
                if (treeMap.size() > N) {
                    treeMap.remove(treeMap.lastKey());
                }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Phone, Long> phoneLongEntry : treeMap.entrySet()) {
                context.write(NullWritable.get(), phoneLongEntry.getKey());
            }
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
        job.getConfiguration().setInt("N", 20);
        job.setJarByClass(TreeMapTopN.class);
        job.setMapperClass(TreeMapTopN.TokenizerMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Phone.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Phone.class);
        job.setCombinerClass(TreeMapTopN.IntSumReducer.class);
        job.setReducerClass(TreeMapTopN.IntSumReducer.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
