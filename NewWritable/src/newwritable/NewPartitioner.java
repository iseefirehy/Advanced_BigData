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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author zhanghongyu
 */
public class NewPartitioner extends Partitioner<NewCompositekey,Text> {
    @Override
    public int getPartition(NewCompositekey key, Text value,int numOfPartitions){
        return (key.getvol().hashCode()%numOfPartitions);
        
    }
}

