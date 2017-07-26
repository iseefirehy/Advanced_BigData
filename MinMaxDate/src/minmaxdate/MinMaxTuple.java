/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package minmaxdate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author zhanghongyu
 */
public class MinMaxTuple implements Writable {
    private double maxprice;
    private String maxsvdate ;
    private String minsvdate ;
    private double maxsv ;
    private double minsv ;
    
    public Double getMaxprice() {
        return maxprice;
    }

    public void setMaxprice(double maxprice) {
        this.maxprice = maxprice;
    }

    public String getMaxsvdate() {
        return maxsvdate;
    }

    public void setMaxsvdate(String maxsvdate) {
        this.maxsvdate = maxsvdate;
    }

    public String getMinsvdate() {
        return minsvdate;
    }

    public void setMinsvdate(String minsvdate) {
        this.minsvdate = minsvdate;
    }

    public Double getMaxsv() {
        return maxsv;
    }

    public void setMaxsv(double maxsv) {
        this.maxsv = maxsv;
    }

    public Double getMinsv() {
        return minsv;
    }

    public void setMinsv(double minsv) {
        this.minsv = minsv;
    }

    @Override
    public void write(DataOutput d) throws IOException {
         WritableUtils.writeString(d, maxsvdate);
         WritableUtils.writeString(d, minsvdate);
        
      d.writeDouble(minsv);
      d.writeDouble(maxsv);
      d.writeDouble(maxprice);

       
    }

    @Override
    public void readFields(DataInput di) throws IOException {
       
        maxsvdate = WritableUtils.readString(di);
        minsvdate = WritableUtils.readString(di);
        maxprice = di.readDouble();
       minsv= di.readDouble();
       maxsv = di.readDouble();
       
        
    }
    
    public String toString(){
        
        return(new StringBuilder().append(Double.toString(maxprice)).append("\t")
                .append(Double.toString(maxsv)).append("\t").append(maxsvdate).append("\t").append(Double.toString(minsv)).append("\t")
                .append(minsvdate).toString());
    }
            
}
