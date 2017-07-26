/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package iphour;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author zhanghongyu
 */
public class IPhour {
    public static class HourMapper extends Mapper<LongWritable,Text,Text,Text>{
        private MultipleOutputs<Text,NullWritable> mos = null;
        private Text outkey = new Text();
        protected void setup(Context context){
            mos = new MultipleOutputs(context);
        }
        public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
            String in = value.toString();
            String IPtime = in.split("\\s")[3];
            String hour = IPtime.substring(13, 15);
            if(hour.equals("00")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "00-tag");
            }else if(hour.equals("01")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "01-tag");
            }else if(hour.equals("02")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "02-tag");
            }else if(hour.equals("03")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "03-tag");
            }else if(hour.equals("04")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "04-tag");
            }else if(hour.equals("05")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "05-tag");
            }else if(hour.equals("06")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "06-tag");
            }else if(hour.equals("07")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "07-tag");
            }else if(hour.equals("08")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "08-tag");
            }else if(hour.equals("09")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "09-tag");
            }else if(hour.equals("10")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "10-tag");
            }else if(hour.equals("11")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "11-tag");
            }else if(hour.equals("12")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "12-tag");
            }else if(hour.equals("13")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "13-tag");
            }else if(hour.equals("14")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "14-tag");
            }else if(hour.equals("15")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "15-tag");
            }else if(hour.equals("16")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "16-tag");
            }else if(hour.equals("17")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "17-tag");
            }else if(hour.equals("18")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "18-tag");
            }else if(hour.equals("19")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "19-tag");
            }else if(hour.equals("20")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "20-tag");
            }else if(hour.equals("21")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "21-tag");
            }else if(hour.equals("22")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "22-tag");
            }else if(hour.equals("23")){
                outkey.set(hour);
                mos.write("bin", value, NullWritable.get(), "23-tag");
            }
        }
            protected void cleanup(Context context) throws IOException, InterruptedException{
                mos.close();
            }
        
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IPhour");
        job.setJarByClass(IPhour.class);
        job.setMapperClass(HourMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        MultipleOutputs.addNamedOutput(job, "bin", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.setCountersEnabled(job, true);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
