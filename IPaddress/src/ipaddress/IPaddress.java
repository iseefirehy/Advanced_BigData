/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipaddress;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author zhanghongyu
 */
public class IPaddress {
    public static class IPMapper extends Mapper<Object, Text,Text,NullWritable>{
        private Text outIP = new Text();
        public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
            String in = value.toString();
            String IPadd = in.split(" ")[0];
            outIP.set(IPadd);
            context.write(outIP, NullWritable.get());
        }
        
    }
    
    
    public static class IPReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        
        public void reduce(Text key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException{
            context.write(key,NullWritable.get());
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IPaddress");
        job.setJarByClass(IPaddress.class);
        job.setMapperClass(IPMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setCombinerClass(IPReducer.class);
        job.setReducerClass(IPReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
