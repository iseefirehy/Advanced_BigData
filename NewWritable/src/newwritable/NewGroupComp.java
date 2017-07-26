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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author zhanghongyu
 */
public class NewGroupComp extends WritableComparator {
    
    protected NewGroupComp()
    {
        super(NewCompositekey.class,true);
    }
    
    public int compare(WritableComparable w1, WritableComparable w2)
    {
        NewCompositekey cw1 = (NewCompositekey) w1;
        NewCompositekey cw2 = (NewCompositekey) w2;
        
        return (cw1.getvol().compareTo(cw2.getvol()));
    }
    
}
