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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRating {

  
  
  public static class MovieRatingMapper1
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
  
  
  
  public static class MovieRatingReducer1
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

   public static class MovieRatingMapper2
            extends Mapper<LongWritable,Text, IntWritable,Text>
    {
        public void map(LongWritable key, Text value, Context context){
            String[] row = (value.toString()).split("\\t");
            Text MovieID = new Text(row[0]);
            String RatingNum = row[1].trim();
            
            try{
                IntWritable count = new IntWritable(Integer.parseInt(RatingNum));
                context.write(count, MovieID);
                
            }catch(Exception e){
                
            }
            
        }
    }
  
 public static class MovieRatingReducer2 
            extends Reducer<IntWritable, Text, IntWritable,Text>{
          
          private TreeMap<IntWritable, Text> movies = new TreeMap<IntWritable, Text>();
          
        public void reduce(IntWritable key, Iterable<Text> value,Context context)
            throws IOException, InterruptedException{
          for(Text val:value){
             
             movies.put(key,val);
             
          }
          
//          while (movies.size() > 25){
//                 movies.remove(movies.firstKey());
//             }
           for (Text t : movies.values()){
               context.write(movies.firstKey(),t);
              }
            
            
           
            
        }
    
 }
  public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "movie rating");
    job1.setJarByClass(MovieRating.class);
    job1.setMapperClass(MovieRatingMapper1.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    
    //job1.setCombinerClass(MovieRatingReducer.class);
    job1.setReducerClass(MovieRatingReducer1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    
    
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    
    boolean complete = job1.waitForCompletion(true);
    
    if(complete){
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2,"Top25");
        job2.setJarByClass(MovieRating.class);
        job2.setMapperClass(MovieRatingMapper2.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        
       
        job2.setReducerClass(MovieRatingReducer2.class);
        

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
        
  }
 }
