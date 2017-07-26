/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipmonth;
import java.io.IOException;
import org.apache.hadoop.conf.Configurable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author zhanghongyu
 */
public class IPmonth {
    public static class MonthMapper extends Mapper<LongWritable, Text,Text,Text>{
        private Text outmon = new Text();
    
        
        
        public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
            String in = value.toString();
            String IPtime = in.split("\\s")[3];
            String mon = IPtime.substring(4, 7);
            outmon.set(mon);
           
            
            context.write(outmon,value);
        }
        
    }
    
    public static class MonthPartitioner extends Partitioner<Text,Text> {
        
        
        
        public int getPartition(Text key, Text value, int numReduceTasks) {
            int par = 0;
             if( key.equals(new Text("Jan"))){
                par =  1;
            }else if( key.equals(new Text("Feb"))){
                par = 2;
            }else if( key.equals(new Text("Mar"))){
                par = 3;
            }else if(key.equals(new Text("Apr"))){
                par = 4;
            }else if( key.equals(new Text("May"))){
                par = 5;
            }else if( key.equals(new Text("Jun"))){
                par = 6;
            }else if( key.equals(new Text("Jul"))){
                par = 7;
            }else if( key.equals(new Text("Aug"))){
                par = 8;
            }else if( key.equals(new Text("Sep"))){
                par = 9;
            }else if( key.equals(new Text("Oct"))){
                par = 10;
            }else if( key.equals(new Text("Nov"))){
                par = 11;
            }else if( key.equals(new Text("Dec"))){
                par = 12;
            }
            return par;
        }

        
    }
    
    
    public static class MonthReducer extends Reducer<Text,Text,Text,Text>{
        private final Text one  =new Text("_");
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
            for(Text t:values){
                context.write(one,t);
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IPbyMonth");
        job.setJarByClass(IPmonth.class);
        job.setMapperClass(MonthMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(MonthPartitioner.class);
        job.setCombinerClass(MonthReducer.class);
        job.setReducerClass(MonthReducer.class);
        job.setNumReduceTasks(13);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
