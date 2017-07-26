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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
//import org.apache.hadoop.mapreduce.Mapper;


public class NewCompositekey implements Writable, WritableComparable<NewCompositekey>{
    private String vol;
    private String price;
    
    public NewCompositekey(){
        
    }
    
    public NewCompositekey(String d,String l){
        this.vol =d ;
        this.price = l;
    }

    public String getvol() {
        return vol;
    }

    public void setvol(String vol) {
        this.vol = vol;
    }

    public String getprice() {
        return price;
    }

    public void setprice(String price) {
        this.price = price;
    }
    
    @Override
    public void readFields(DataInput di)throws IOException{
        vol = WritableUtils.readString(di);
        price = WritableUtils.readString(di);
    }
    
    @Override
    public void write(DataOutput d)throws IOException{
        WritableUtils.writeString(d,vol);
        WritableUtils.writeString(d,price);
    }
    @Override
    public int compareTo(NewCompositekey o){
        int result = vol.compareTo(o.vol);
        if(result ==0)
        {
            result = price.compareTo(o.price);
        }
        return result;
    }
    
    public String toString(){
        return(new StringBuilder().append(vol).append("\t").append(price).toString());
    }

    

    
}
