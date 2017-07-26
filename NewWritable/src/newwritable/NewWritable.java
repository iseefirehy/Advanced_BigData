/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package newwritable;

/**
 *
 * @author zhanghongyu
 */
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author zhanghongyu
 */
public class NewWritable {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
     try{
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "New");
    job.setJarByClass(NewWritable.class);
    
    job.setMapperClass(NewMapper.class);
    job.setMapOutputKeyClass(NewCompositekey.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setGroupingComparatorClass(NewGroupComp.class);
    
    job.setReducerClass(NewReducer.class);
    job.setOutputKeyClass(NewCompositekey.class);
    job.setOutputValueClass(Text.class);
    job.setPartitionerClass(NewPartitioner.class);
    
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(8);

    System.exit(job.waitForCompletion(true)? 0 : 1);// TODO code application logic here
    } catch (IOException |InterruptedException | ClassNotFoundException ex){
       System.out.println("Error Message" + ex.getMessage());
    }
    
    }
    }
   
