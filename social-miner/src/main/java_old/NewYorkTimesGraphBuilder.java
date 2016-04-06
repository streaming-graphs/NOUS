import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NewYorkTimesGraphBuilder {
	
    public static class UserCountMap extends Mapper<LongWritable, Text, Text, IntWritable>  {
	    public void map(LongWritable key, Text Value, Context context) 
	                    throws IOException, InterruptedException {
	        String line = Value.toString() ;
	        String tokens[] = line.split("\t") ;  
	        for (String t:tokens) {
	        	context.write(new Text(t), new IntWritable(1));
	        }
//	        if (tokens.length > 0) {                
//	            context.write(new Text(tokens[0]),new IntWritable(1)) ;                
//	        }
	    }      
	}

	public static class UserCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	                throws IOException, InterruptedException {
	    	int count = 0 ;       
	        for (IntWritable value : values) {
	            count = count + value.get() ;
	        }
	        context.write(key, new IntWritable(count)) ;
	    }    
	}

public static void main(String[] args) throws Exception {
	for (String s:args) {
		System.out.println(s);
	}
    Job job = Job.getInstance();
    job.setJarByClass(NewYorkTimesGraphBuilder.class) ;        
    FileInputFormat.addInputPath(job, new Path(args[1])) ;
    FileOutputFormat.setOutputPath(job, new Path(args[2])) ;        
    job.setMapperClass(UserCountMap.class) ;
    job.setReducerClass(UserCountReducer.class) ;        
    job.setOutputKeyClass(Text.class) ;
    job.setOutputValueClass(IntWritable.class) ;
    System.exit(job.waitForCompletion(true) ? 0 : 1) ;        
}    
//	
//	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
//	    private final static IntWritable one = new IntWritable(1);
//	    private Text word = new Text();
//
//		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//	        String line = value.toString();
//	        StringTokenizer tokenizer = new StringTokenizer(line);
//	        while (tokenizer.hasMoreTokens()) {
//	            word.set(tokenizer.nextToken());
//	            context.write(word, one);
//	        }
//	    }
//	}
//	
//	 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//		    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
//		      throws IOException, InterruptedException {
//		        int sum = 0;
//		        for (IntWritable val : values) {
//		            sum += val.get();
//		        }
//		        context.write(key, new IntWritable(sum));
//		    }
//		 }
////	public void Map(String line) {
////		
////		String[] tokens = GetKeyFeatures(line);
////		String entity = tokens[0];
////		for (int i = 1, N = tokens.length; i <  N; i++) {
////			Emit(tokens[i], entity);
////		}
////	}
////	
////	public void Reduce() {
////		int numEntities;
////		for (int i = 0; i < numEntities-1; i++) {
////			for (int j = i+1; j < numEntities; j++) {
////				entity1 = entities[i];
////				entity2 = entities[j];
////				if (entity1 < entity2) {
////					edgeKey = entity1 + "\t" + entity2;
////					emit(edgeKey, 1);
////				}
////			}
////		}
////	}
//
//	public int run(Context context) throws Exception {
//        setup(context);
//        while (context.nextKeyValue()) {
//              map(context.getCurrentKey(), context.getCurrentValue(), context);
//            }
//        cleanup(context);
//		return 0;
//	}
}
