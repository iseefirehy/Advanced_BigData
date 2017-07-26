/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ratingavg;

/**
 *
 * @author zhanghongyu
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
public class RatingAvg {
       public static class RatingMap extends Mapper<Object,Text,Text,DoubleWritable>{
           public void map(Object key, Text value, Context context) 
                   throws IOException, InterruptedException{
               try{
                   String row[] = value.toString().split("::");
               
                
                String MovieID = row[1];
                String Ratings= row[2];
                
                double score= Double.parseDouble(Ratings);
                context.write(new Text(MovieID),new DoubleWritable(score));
               }catch(Exception e){
                   System.out.println("Error Message");
            }  
           }
       }
           
           public static class RatingReducer extends Reducer<Text,DoubleWritable,Text,RatingWritable>
           {
               private RatingWritable result = new RatingWritable();
               private ArrayList<Double> list  = new ArrayList<Double>();

               @Override
               protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
                    //To change body of generated methods, choose Tools | Templates.
                    double sum=0;
                    double count = 0;
                    
                    list.clear();
                    result.setStddev(0);
                    
                    for(DoubleWritable val : values){
                        list.add(val.get());
                        sum += val.get();
                        count++;
                    }
                    Collections.sort(list);
                    if(count == 0){
                        
                    }else if(count == 1){
                        result.setMedian(list.get(0));
                    }
                    else if(count % 2 ==0){
                        result.setMedian((list.get((int)count/2-1) + list.get((int)count/2))/2);
                        
                    }else{
                        result.setMedian(list.get((int)count/2-1));
                    }
                    
                    double mean = sum/count;
                    double sumOfSquares = 0;
                    
                    for(double val:list){
                        sumOfSquares += (val-mean) * (val-mean);
                    }
                    result.setStddev((double)Math.sqrt(sumOfSquares/(count-1)));
                    context.write(key,result);
               }
               
               
               
           }           
       
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "RatingMeanSTD");
        job.setJarByClass(RatingAvg.class);
    
        job.setMapperClass(RatingMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
    
    
        job.setReducerClass(RatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RatingWritable.class);
    
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
    
}
