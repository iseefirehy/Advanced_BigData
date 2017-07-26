/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package titletag;


import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author zhanghongyu
 */
public class TitleTag {

    

    public static class TitleMapper extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String in = value.toString();
            String movieId = in.split(",")[0];
            String title = in.split(",")[1];
            if (movieId == "movieId" && title == "title") {

            } else {
                outkey.set(movieId);
                outvalue.set("B" + title);
                context.write(outkey, outvalue);
            }
        }
    }
public static class TagMapper extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String in = value.toString();
            String movieId = in.split(",")[1];
            String tag = in.split(",")[2];
            if (movieId == "movieId" && tag == "tag") {

            } else {
                outkey.set(movieId);
                outvalue.set("A" + tag);
                context.write(outkey, outvalue);
            }
        }
    }
    public static class TitleTagReducer extends Reducer<Text, Text, Text, NullWritable> {

        private ArrayList<String> tags = new ArrayList<String>();
        private  DocumentBuilderFactory dbf =  DocumentBuilderFactory.newInstance();
        private String titles = null;
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
            titles = null;
            tags.clear();
            for(Text t:values){
                if(t.charAt(0) == 'B'){
                    titles = t.toString().substring(1).trim();
                }else{
                    tags.add(t.toString().substring(1).trim());
                }
            }
            if(titles != null){
                String titleswithtagchildren;
                try {
                    titleswithtagchildren = nestElements(titles,tags);
                    context.write(new Text(titleswithtagchildren),NullWritable.get());
                } catch (ParserConfigurationException ex) {
                    Logger.getLogger(TitleTag.class.getName()).log(Level.SEVERE, null, ex);
                } catch (SAXException ex) {
                    Logger.getLogger(TitleTag.class.getName()).log(Level.SEVERE, null, ex);
                } catch (TransformerException ex) {
                    Logger.getLogger(TitleTag.class.getName()).log(Level.SEVERE, null, ex);
                }
                
                
            }
        }
        private String nestElements(String titles, ArrayList<String> tags) throws ParserConfigurationException, SAXException, IOException, TransformerException{
            DocumentBuilder bldr = dbf.newDocumentBuilder(); 
            Document doc = bldr.newDocument();
            
            //Element titleEL = getXmlElementfromString(titles);
            Element toAddtitleEL = doc.createElement("movietitles");
            toAddtitleEL.setAttribute("titles", titles);
            //copyAttributesToElement(titleEL.getAttributes(),toAddtitleEL);
            
            for(String tagxml: tags){
                //Element tagEL = getXmlElementfromString(tagxml);
                Element toAddtagEL = doc.createElement("movietags");
                toAddtagEL.setAttribute("tag", tagxml);
                //copyAttributesToElement(tagEL.getAttributes(),toAddtagEL);
                toAddtitleEL.appendChild(toAddtagEL);
                

            }
            doc.appendChild(toAddtitleEL);
            return transformDocumentToString(doc);
        }
        private Element getXmlElementfromString(String xml) throws ParserConfigurationException, SAXException, IOException{
            DocumentBuilder bldr = dbf.newDocumentBuilder();
            
            return bldr.parse(new InputSource(new StringReader(xml))).getDocumentElement();
        }
        private void copyAttributesToElement(NamedNodeMap attributes,Element element){
            for(int i = 0;i < attributes.getLength();++i){
                Attr toCopy=(Attr) attributes.item(i);
                element.setAttribute(toCopy.getName(),toCopy.getValue());
            }
        }
        
        private String transformDocumentToString(Document doc) throws TransformerConfigurationException, TransformerException{
            
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(writer));
            
            return writer.getBuffer().toString().replaceAll("\n|\r", "");
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "TitleTagHierarchy");
        job.setJarByClass(TitleTag.class);

        // Use MultipleInputs to set which input uses what mapper
        // This will keep parsing of each data set separate from a logical
        // standpoint
        // The first two elements of the args array are the two inputs
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TitleMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TagMapper.class);
        job.getConfiguration().set("join.type", "inner");
        //job.setNumReduceTasks(0);
        job.setReducerClass(TitleTagReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 2 );
    }

}
