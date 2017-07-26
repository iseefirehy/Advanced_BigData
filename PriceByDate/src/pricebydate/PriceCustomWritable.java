/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pricebydate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


/**
 *
 * @author zhanghongyu
 */
public class PriceCustomWritable implements Writable {
        
        private float price;
        private float count;
        
        public PriceCustomWritable()
    {
        
    }
    
    public PriceCustomWritable(Float count,Float price)
    {
        
        this.price= price;
        this.count = count;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }
    
        
    


    
    public float getCount() {
        return count;
    }

    public void setCount(float count) {
        this.count = count;
    }
    
    
    
    
    @Override
    public void write(DataOutput d) throws IOException {
       d.writeFloat(price);
        d.writeFloat(count);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
       price = di.readFloat();
       
       count = di.readFloat();
    }
        @Override
    public String toString(){
        return (new StringBuilder().append(count).append("\t").append(price).toString());
    }

    

    
}
