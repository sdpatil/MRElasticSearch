package com.spnotes.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.slf4j.*;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by gpzpati on 5/10/14.
 */
public class WordCount extends Configured implements Tool{


    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        Logger logger = LoggerFactory.getLogger(WordCountMapper.class);
        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            logger.debug("Entering WordCountMapper.map() " + this);
            String line = value.toString();
            StringTokenizer st = new StringTokenizer(line," ");
            while(st.hasMoreTokens()){
                word.set(st.nextToken());
                context.write(word,one);
            }
            logger.debug("Exiting WordCountMapper.map()");
        }

    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, MapWritable> {
        Logger logger = LoggerFactory.getLogger(WordCountReducer.class);
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {
            logger.debug("Entering WordCountReducer.reduce() " + this);

            int sum = 0;
            Iterator<IntWritable> valuesIt = values.iterator();
            while(valuesIt.hasNext()){
                sum = sum + valuesIt.next().get();
            }
            logger.debug(key + " -> " + sum);
            MapWritable mapWritable = new MapWritable();
            mapWritable.put(key, new IntWritable(sum));
            context.write(key, mapWritable);
            logger.debug("Exiting WordCountReducer.reduce()");
        }
    }

    Logger logger = LoggerFactory.getLogger(WordCount.class);

    public static void main(final String[] args) throws Exception{

        System.setProperty("HADOOP_USER_NAME", "hdfs");

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hdfs");
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            public Void run() throws Exception {
                Configuration configuration = new Configuration();
                configuration.set("hadoop.job.ugi", "hdfs");
                ToolRunner.run(new WordCount(), args);
                return null;
            }
        });


    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job();



        job.setJarByClass(WordCount.class);
        job.setJobName("WordCounter");
        logger.info("Input path " + args[0]);
        logger.info("Oupput path " + args[1]);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        Configuration configuration = job.getConfiguration();
        configuration.set("es.nodes","localhost:9200");
        configuration.set("es.resource","hadoop/wordcount2");

        job.setOutputFormatClass(EsOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        int returnValue = job.waitForCompletion(true) ? 0:1;
        System.out.println("job.isSuccessful " + job.isSuccessful());
        return returnValue;
    }


}
