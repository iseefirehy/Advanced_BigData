/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mergefiles;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 *
 * @author zhanghongyu
 */
public class MergeFiles {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        // TODO code application logic here
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileSystem local = FileSystem.getLocal(conf);
        
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        
        try{
            FileStatus[] inputFiles = local.listStatus(input);
            FSDataOutputStream out = fs.create(output);
            
            for(int i = 0; i <inputFiles.length;i++){
                FSDataInputStream in  = local.open(inputFiles[i].getPath());
                byte buff[] = new byte[256];
                int bytesRead = 0;
                while((bytesRead = in.read(buff))>0){
                    out.write(buff,0,bytesRead);
                }
                in.close();
                
            }
            out.close();
        }catch(IOException e){
            System.out.print("Wrong!");
            
        }
        
        
    }
    
}
