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


//import com.sun.istack.internal.logging.Logger;
//import sun.util.logging.PlatformLogger;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 *
 * @author zhanghongyu
 */
public class NewMapper extends Mapper<Object,Text, NewCompositekey,Text> {
    
    public void map(Object key,Text values,Context context)
    {
        if(values.toString().length()>0)
        {
            try{
                String value[] =  values.toString().split(",");
                NewCompositekey cw = new NewCompositekey(value[7],value[8]);
                Text t = new Text(value[2]);
                context.write(cw, t);
            }catch(IOException | InterruptedException ex){
            //Logger.getLogger(Lab2Mapper.class.getName().log(Level.SEVERE,null,ex));
            }
        }
    }
}

