/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package combinewordcount;

/**
 *
 * @author zhanghongyu
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CombineWordCount {

  
  
  public static class CWCMapper
       extends Mapper<Object, Text, Text, FloatWritable>{

    private final static FloatWritable price  = new FloatWritable();
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String in = value.toString();
      String stock_sym = in.split(",")[1];
      String stock_price = in.split(",")[4];
      if(stock_sym =="stock_symbol" && stock_price == "stock_price_high" ){
         
      }else{
          try{
          float p = Float.parseFloat(stock_price);
          price.set(p);
          word.set(stock_sym);
          context.write(word,price);
          }catch(Exception e){
              
          }
      }
    }
  }
  
  
  
  public static class CWCReducer
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      float sum = 0;
      float counter = 0;
      for (FloatWritable val : values) {
        sum += val.get();
        counter++;
      }
      float avg = sum/counter;
      result.set(avg);
      context.write(key, result);
    }
  }


  public static void main(String[] args) throws Exception {
   
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "stock price");
    job.setJarByClass(CombineWordCount.class);
    job.setMapperClass(CWCMapper.class);
    job.setCombinerClass(CWCReducer.class);
    job.setReducerClass(CWCReducer.class);
    job.setInputFormatClass(CombineFileInputFormat.class);
    CombineFileInputFormat.setMaxInputSplitSize(job, 10000000);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    CombineFileInputFormat.addInputPath(job, new Path(args[0]));
    CombineFileOutputFormat.setOutputPath(job, new Path(args[1]));
   
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
    
  }
}