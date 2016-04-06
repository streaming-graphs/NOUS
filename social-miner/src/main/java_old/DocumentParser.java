import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.twitter.Extractor;

public class DocumentParser
{

    public static class DocParseMap extends
            Mapper<LongWritable, Text, Text, Text>
    {

        public void map(LongWritable key, Text Value, Context context)
                throws IOException, InterruptedException
        {
            String line = Value.toString();
            List names = null;
            Extractor extractor = new Extractor();
            String valuestr = "";
            String keystr = "";
            JSONObject obj = null;
            HashSet<String> mentioned = new HashSet<String>();
            try
            {
                obj = new JSONObject(line);
                try
                {
                    keystr += "<doc_" + obj.getString("id") + ">";
                    valuestr += "<hasID>,\"" + obj.getString("id") + "\"";
                    valuestr += "\t<hasAuthor>,\"" + obj.getString("author")
                            + "\"";

                    try
                    {
                        JSONArray hashtags = obj.getJSONArray("hashtags");
                        for (int i = 0; i < hashtags.length(); i++)
                        {
                            valuestr += "\t<hasHashtag>,\""
                                    + hashtags.get(i).toString() + "\"";
                        }
                    } catch (JSONException e1)
                    {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                    
                    try
                    {
                        JSONArray mentionedArray = obj.getJSONArray("mentions");
                        for (int i = 0; i < mentionedArray.length(); i++)
                        {
                            mentioned.add(mentionedArray.get(i).toString());
                        }
                    } catch (JSONException e1)
                    {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                    try
                    {
                        valuestr += "\t<hasDate>,\"" + obj.getString("date") + "\"";
                    }catch (JSONException e1)
                    {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                    try
                    {
                        valuestr += "\t<hasAuthorScreenName>,\""
                            + obj.getString("screenname") + "\"";
                    }catch (JSONException e1)
                    {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                    try
                    {
                        valuestr += "\t<hasBeenRetweeted>,\""
                            + obj.getString("retweet") + "\"";
                    }catch (JSONException e1)
                    {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                    JSONObject doc = null;
                    JSONObject reclang = null;
                    try
                    {
                        doc = new JSONObject(obj.getString("document"));
                        try
                        {
                        reclang = new JSONObject(doc.getString("reclang"));
                        valuestr += "\t<hasLanguage>,\""
                                + reclang.getString("language") + "\"";
                        }
                        catch (JSONException e1)
                        {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                        try
                        {
                            valuestr += "\t<hasText>,\"" + doc.getString("text") + "\"";
                        }catch (JSONException e1)
                        {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                        try
                        {
                        names = extractor.extractMentionedScreennames(doc
                                .getString("text"));
                        for (int i = 0; i < names.size(); i++)
                        {
                            String name = (String) names.get(i);
                            mentioned.add("@" + name);

                        }
                        Iterator<String> it = mentioned.iterator();
                        while (it.hasNext())
                        {
                            valuestr += "\t<hasMentioned>,\"" + it.next() + "\"";

                        }
                        }catch (JSONException e1)
                        {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                        try
                        {
                            List<String> urls = extractor
                                    .extractURLs(doc.getString("text"));
                            for (int i = 0; i < urls.size(); i++)
                            {
                                valuestr += "\t<hasReferencedURL>,\""
                                        + urls.get(i).toString() + "\"";

                            }

                        } catch (JSONException e)
                        {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } 
                        } catch (JSONException e)
                        {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
    
    
                        context.write(new Text(keystr), new Text(valuestr));

                } catch (JSONException e1)
                {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
           } catch (JSONException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
    }

    public static class DocParseReducer extends
            Reducer<Text, Iterable<Text>, NullWritable, Text>
    {
        public void reduce(Text key, Text values, Context context)
                
        {
            try
            {
                context.write(NullWritable.get(), values);
            } catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        Job job = Job.getInstance();
        job.setJarByClass(DocumentParser.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(DocParseMap.class);
        job.setReducerClass(DocParseReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
