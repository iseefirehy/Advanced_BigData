/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package movierating;

/**
 *
 * @author zhanghongyu
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRating {

  
  
  public static class MovieRatingMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one  = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value,Context context
                    ) throws IOException, InterruptedException {
      String in = value.toString();
      String MovieID = in.split("::")[1];
      word.set(MovieID);
      context.write(word,one);
    } 
  }
  
  
  
  public static class MovieRatingReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
        
      }
      if (sum > 1){
      result.set(sum);
      context.write(key, result);
        }
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "movie rating");
    job.setJarByClass(MovieRating.class);
    job.setMapperClass(MovieRatingMapper.class);
    job.setCombinerClass(MovieRatingReducer.class);
    job.setReducerClass(MovieRatingReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}