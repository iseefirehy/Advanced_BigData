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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author zhanghongyu
 */
public class RAMCWritable implements Writable {
    private float median;
    private float stddev;
    

    public RAMCWritable() {
    }
    
    

   
    
    
    
    public float getMedian() {
        return median;
    }

    public void setMedian(float median) {
        this.median = median;
    }

    public float getStddev() {
        return stddev;
    }

    public void setStddev(float stddev) {
        this.stddev = stddev;
    }


    @Override
    public void write(DataOutput d) throws IOException {
        d.writeDouble(median); //To change body of generated methods, choose Tools | Templates.
        d.writeDouble(stddev);
       
    }

    @Override
    public void readFields(DataInput di) throws IOException {
       median = di.readFloat(); //To change body of generated methods, choose Tools | Templates.
       stddev = di.readFloat();
       
    }
    public String toString(){
        return(this.getMedian()+ "\t" + this.getStddev());
    }
}
