/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pricebydate;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable; 

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
public class PriceByDate {
    public static class PriceMapper extends Mapper<Object, Text,Text,PriceCustomWritable>{
     
     //private PriceCustomWritable outPriceAvg = new PriceCustomWritable();
     private FloatWritable outprice ;
     private Text timein;
     public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
         String in = value.toString();
         String years = in.split(",")[2];
         String price = in.split(",")[8];
         
         if(years =="date" && price == "stock_price_adj_close" ){
         
      }else{
             try{
         String year = years.substring(0,4);
         Float stockprice = Float.parseFloat(price);
         timein = new Text(year);
         
         
         PriceCustomWritable pw = new PriceCustomWritable(1.0f,stockprice);
         context.write(timein,pw);
             }catch(Exception e){
                 
             }
     }
     }
    }
    
    public static class PriceReducer extends Reducer<Text,PriceCustomWritable,
            Text,PriceCustomWritable>{
            //private PriceCustomWritable result = new PriceCustomWritable();
            private PriceCustomWritable out = new PriceCustomWritable();
            public void reduce(Text key,Iterable<PriceCustomWritable> values,Context context) throws IOException, InterruptedException{
            float sum = 0;
            float count = 0;
            
            for(PriceCustomWritable val:values){
                sum += val.getPrice()*val.getCount();
                count += val.getCount();
            }
           
            out.setCount(count);
            
            out.setPrice(sum/count);
             
            
            context.write(key, out);
            
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "PriceByDate");
    job.setJarByClass(PriceByDate.class);
    
    job.setMapperClass(PriceMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PriceCustomWritable.class);
    
    job.setCombinerClass(PriceReducer.class);

    job.setReducerClass(PriceReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PriceCustomWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

