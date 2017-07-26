/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ratingavgmemoryc;

/**
 *
 * @author zhanghongyu
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 *
 * @author zhanghongyu
 */
public class RatingAvgMemoryC {
       public static class MemoryMap extends Mapper<Object,Text,Text,SortedMapWritable>{
          private static final FloatWritable ONE = new FloatWritable(1);
          private FloatWritable scores ;
           public void map(Object key, Text value, Context context) 
                   throws IOException, InterruptedException{
               try{
                   String row[] = value.toString().split("::");
               
                
                String MovieID = row[1];
                String Ratings= row[2];
                
                Float score= Float.parseFloat(Ratings);
                scores = new FloatWritable(score);
                SortedMapWritable outrating  =new SortedMapWritable();
                outrating.put(scores,ONE);
                context.write(new Text(MovieID),outrating);
               }catch(Exception e){
                   System.out.println("Error Message");
            }  
           }
       }
           public static class MemoryCombiner extends Reducer<Text,SortedMapWritable,Text,SortedMapWritable>{
            
               protected void reduce(Text key,Iterable<SortedMapWritable> value,Context context)
                   throws IOException, InterruptedException{
                   SortedMapWritable outValue = new SortedMapWritable();
                   for(SortedMapWritable v:value){
                       for(Entry<WritableComparable,Writable> entry:v.entrySet()){
                           FloatWritable count = (FloatWritable) outValue.get(entry.getKey());
                           
                           if(count != null){
                               count.set(count.get()+
                                      ((FloatWritable)entry.getValue()).get());
                           }else{
                               outValue.put(entry.getKey(),new FloatWritable(
                               ((FloatWritable)entry.getValue()).get()));
                           }
                       }
                       v.clear();
               }
                   context.write(key, outValue);
               } 
           }
           public static class MemoryReducer extends Reducer<Text,SortedMapWritable,Text,RAMCWritable>
           {
               private RAMCWritable result = new RAMCWritable();
               private TreeMap<Float,Float> ratingcount= new TreeMap<Float,Float>(); 
 
               protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
                    //To change body of generated methods, choose Tools | Templates.
                    float sum=0;
                    float count = 0;
                    
                    ratingcount.clear();
                    result.setStddev(0);
                    result.setMedian(0);
                    
                    for(SortedMapWritable val : values){
                       for(Entry<WritableComparable, Writable> entry:val.entrySet()){
                           float ratings =((FloatWritable) entry.getKey()).get();
                           float counter = ((FloatWritable)entry.getValue()).get();
                           
                           count += counter;
                           sum += ratings*counter;
                           
                           Float storedrating = ratingcount.get(counter);
                           if(storedrating == null){
                               ratingcount.put(ratings,counter);
                           }else{
                               ratingcount.put(ratings,storedrating+counter);
                           }
                       }
                    }
                    float medindex = count/2.0f;
                    float previousrating = 0;
                    float rate = 0;
                    float prekey = 0;
                    
                    for(Entry<Float,Float>entry: ratingcount.entrySet()){
                        rate = previousrating + entry.getValue();
                        if(previousrating <= medindex && medindex < rate){
                            if(count%2==0 && previousrating == medindex){
                                result.setMedian((entry.getKey()+prekey)/2.0f);
                            }else{
                                result.setMedian(entry.getKey());
                            }
                            break;
                        }
                        previousrating = rate;
                        prekey = entry.getKey();
                    }
                    
                    
                    
                    float mean = sum/count;
                    float sumOfSquares = 0;
                    for(Entry<Float,Float> entry:ratingcount.entrySet()){
                        sumOfSquares += (entry.getKey()-mean) * (entry.getKey()-mean)*entry.getValue();
                    }
                    result.setStddev((float)Math.sqrt(sumOfSquares/(count-2)));
                    context.write(key, result);    
               }  
           }           
       
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "RatingMeanSTD");
        job.setJarByClass(RatingAvgMemoryC.class);
    
        job.setMapperClass(MemoryMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SortedMapWritable.class);
    
        job.setCombinerClass(MemoryCombiner.class);
        job.setReducerClass(MemoryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RAMCWritable.class);
    
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
    
}
