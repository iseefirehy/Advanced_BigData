/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package minmaxdate;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class MinMaxDate {
    public static class MMMapper extends Mapper<Object,Text,Text,MinMaxTuple>{
        private Text word = new Text();
        private MinMaxTuple outTuple = new MinMaxTuple();
        
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
            String in = value.toString();
            String stockname = in.split(",")[1];
            String dates = in.split(",")[2];
            String volume = in.split(",")[7];
            String stock_price = in.split(",")[8];
      if(stockname =="stock_symbol" && stock_price == "stock_price_adj_close" && volume == "stock_volume" && dates =="date"){
         
      }else{
          try{
          double p = Double.parseDouble(stock_price);
          double vol = Double.parseDouble(volume);
          outTuple.setMaxprice(p);
          outTuple.setMaxsvdate(dates);
          outTuple.setMinsvdate(dates);
          outTuple.setMaxsv(vol);
          outTuple.setMinsv(vol);
          word.set(stockname);
          context.write(word,outTuple);
          }catch(Exception e){
              
          }
        }
      }
    }
    
    public static class MMReducer extends Reducer<Text,MinMaxTuple,Text,MinMaxTuple>{
        private MinMaxTuple result = new MinMaxTuple();
        
        public void reduce(Text key, Iterable<MinMaxTuple> values,Context context)
                throws IOException,InterruptedException{
                result.setMaxprice(0);
                result.setMaxsv(0);
                result.setMaxsvdate(null);
                result.setMinsvdate(null);
                result.setMinsv(0);
                double counter = 0.0;
                for(MinMaxTuple val:values){
                    
                     if(val.getMaxprice().compareTo(result.getMaxprice())>0){
                        result.setMaxprice(val.getMaxprice());
                    }
                    if(result.getMinsv() == 0.0 && counter ==0.0){ 
                        result.setMinsv(val.getMinsv());
                        result.setMinsvdate(val.getMinsvdate());
                    }else if(val.getMinsv().compareTo(result.getMinsv())<0){
                    result.setMinsv(val.getMinsv());
                    result.setMinsvdate(val.getMinsvdate());        
                }
                    if(result.getMaxsv() == 0.0 && counter ==0.0){  
                        result.setMaxsv(val.getMaxsv());
                        result.setMaxsvdate(val.getMaxsvdate());
                    }else if(val.getMaxsv().compareTo(result.getMaxsv())>0){
                        result.setMaxsv(val.getMaxsv());
                        result.setMaxsvdate(val.getMaxsvdate());
                        }
                    
                    
                }
                context.write(key,result);
                
        }
        
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here 
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stock_date");
        job.setJarByClass(MinMaxDate.class);
        
        job.setMapperClass(MMMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MinMaxTuple.class);
        
        job.setReducerClass(MMReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxTuple.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
