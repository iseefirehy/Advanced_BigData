/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lab4_std_dev;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author zhanghongyu
 */
public class MedianSDCustomWritable implements Writable {
    private double median;
    private double stddev;

    public double getMedian() {
        return median;
    }

    public void setMedian(double median) {
        this.median = median;
    }

    public double getStddev() {
        return stddev;
    }

    public void setStddev(double stddev) {
        this.stddev = stddev;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeDouble(median); //To change body of generated methods, choose Tools | Templates.
        d.writeDouble(stddev);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
       median = di.readDouble(); //To change body of generated methods, choose Tools | Templates.
       stddev = di.readDouble();
    }
    public String toString(){
        return(this.getMedian()+ "\t" + this.getStddev());
    }
}
