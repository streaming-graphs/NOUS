import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class DocumentCluster
{
    
    public static class DocClusterMap extends
    Mapper<LongWritable, Text, Text, Text>
    {
        
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
        {
         
         String shingle = "";   
            
         int P = 104395301; //http://primes.utm.edu/lists/small/millions/
         int S = 2;
         int C = 3;
         int[][] randomNumPairs = new int[C][2];
         Random r = new Random();
         for(int i =0;i < C;i++)
         {
             randomNumPairs[i][0]= r.nextInt();
             randomNumPairs[i][1]= r.nextInt();
         }
         
         
         String line = values.toString();
         //<tweet_1383289200202571823>     <hasID>,"1383289200202571823"   <hasAuthor>,"#Jazmin"   <hasDate>,"2013-11-01T06:59:44Z"        
         String[] st = line.split("\t");
         HashMap<String, String> Tao_U_Hash = new HashMap<String, String>();
         for(int i=1;i<st.length;i++) // 0th entry is doc id
         {
             String edge = st[i];
             String[] edgeArray = edge.split(",");
             if(edgeArray.length > 0 && (edgeArray[0].equals("<hasHashtag>") || 
                     edgeArray[0].equals("<hasMentioned>") ||
                     edgeArray[0].equals("<hasReferencedURL>") ))
                 Tao_U_Hash.put(edgeArray[1], edgeArray[0]);
             //added .put("#Jazmin",<hasAuthor>);
         }
         ArrayList<TreeMap<Integer,String>> Tao_U_All_J = new ArrayList<TreeMap<Integer,String>>(C);
         for(int i = 0; i < C ;i++)
         {
             TreeMap<Integer,String> Tao_U_J = new TreeMap<Integer,String>();
             for(String v:Tao_U_Hash.keySet())
             {
                 Tao_U_J.put((randomNumPairs[i][0] * v.hashCode() + randomNumPairs[i][1])%P,v);
             }
             
             Tao_U_All_J.add(Tao_U_J);
             Set<Integer> tau_u_jkeys = Tao_U_J.keySet();
             Iterator<Integer> it = tau_u_jkeys.iterator();
             for(int j =0 ; j < S && it.hasNext();j++)
             {
                 shingle += Tao_U_J.get(it.next()).toString() + " ,";
             }
             int shingleid = shingle.hashCode();
             shingle = "<" +shingle + " " + st[0] + " >";
             context.write(new Text(""+shingleid+""), new Text(shingle));
         }
        }
    }
    
    public static class DocClusterReducer extends
    Reducer<Text, Text, NullWritable, Text>
    {
        public void reduce(Text  key, Iterable<Text> values0, Context context)
                throws IOException, InterruptedException
        {
            
            //TreeMap <Integer,String> shinglemap = new TreeMap<Integer, String>(); 
            //System.out.println(" values ize is " + values.hasNext());
            //System.out.println(" values ize is " + values.hasNext());
            //2145807083 : <"#Ironic" , <tweet_1383296017095993915> > 
            Iterator<Text> shingleIt = values0.iterator();
            String outputgraphline = "";
            while(shingleIt.hasNext())
            {
                Text shingleLine = shingleIt.next();
                //shingleLine is <"#Ironic" , <tweet_1383296017095993915> >
                String shingleidvalueline = shingleLine.toString();
                shingleidvalueline = shingleidvalueline.replaceAll("<", "");
                shingleidvalueline = shingleidvalueline.replaceAll(">", "");
                
                String[] shinglevaluearray = shingleidvalueline.split(",");
                try{
                    String sourceU = shinglevaluearray[shinglevaluearray.length-1];
                    
                    //System.out.println(" shingle source is  : "+sourceU);
                    for(int i = 0 ; i < shinglevaluearray.length - 1; i ++)
                    {
                        outputgraphline +=  sourceU + " generated " + shinglevaluearray[i] + "\n";
                    }
                }catch(Exception e)
                {
                    System.out.println(" Exception catch line is " + shingleidvalueline);
                    System.exit(-1);
                }
            }
            
            
            context.write(NullWritable.get(), new Text(outputgraphline));
            
            
            
            /*
            Iterator<Text> values = values0.iterator();
            while(values.hasNext())
            {
                String line = values.next().toString();
               // System.out.println(" line is : "+line);
                String[] valueArr = line.split(":");
                if((valueArr != null) && (valueArr.length > 0))
                {
                    String shingleid = valueArr[0]; 
                    //id is : 2145807083;
                    shinglemap.put(new Integer(shingleid.trim()), line.replace(shingleid,"").replace(":", "").trim());
                    //.put(2145807083 , <"#Ironic" , <tweet_1383296017095993915> >)
                }
                
            }
            Iterator<Integer> shingleidkeyset = shinglemap.keySet().iterator();
            String outputgraphline = "";
            
            while(shingleidkeyset.hasNext())
            {
                String shingleidvalueline = shinglemap.get(shingleidkeyset.next());
                String[] shinglevaluearray = shingleidvalueline.split(",");
                
                try{
                    String sourceU = shinglevaluearray[shinglevaluearray.length-1];
                    //System.out.println(" shingle source is  : "+sourceU);
                    for(int i = 0 ; i < shinglevaluearray.length - 1; i ++)
                    {
                        outputgraphline +=  sourceU + " generated " + shinglevaluearray[1] + "\n";
                    }
                }catch(Exception e)
                {
                    System.out.println(" Exception catch line is " + shingleidvalueline);
                    System.exit(-1);
                }
                
            }
            
            context.write(NullWritable.get(), new Text(outputgraphline));
            */
        }
    }
    public static void main(String[] args) throws Exception
    {
        Job job = Job.getInstance();
        job.setJarByClass(DocumentParser.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(DocClusterMap.class);
        job.setReducerClass(DocClusterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
