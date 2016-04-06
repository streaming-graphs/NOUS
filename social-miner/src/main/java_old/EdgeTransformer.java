import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;


public class EdgeTransformer {

	public static class EdgeMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text("e");
		private Text outValue = new Text();
		private HashMap<String, String> idMap = new HashMap<String, String>();
		
		private String vertexIdPath;
		
//		public void configure(JobConf conf) {
//			
//			try {
//				String vertexIdPath = conf.get("VertexIdPath");
//				FileSystem fs = FileSystem.get(conf);
//				FileStatus[] files = fs.listStatus(new Path(vertexIdPath));
//				Integer partition_id = conf.getInt("mapred.task.partition", 0);
//				
//				for (FileStatus f : files) {
//					if (f.getPath().getName().toString().contains("part") == false) continue;
//					PrintWriter p = new PrintWriter(new FileWriter("/data/hadoop/logs/EdgeMapper.log." + partition_id.toString()));
//					
//					p.println("Reading ids from = " + f.getPath().toString());
//					p.flush();
//					BufferedReader reader = new BufferedReader(new InputStreamReader(
//							fs.open(f.getPath())));
//					String line = null;
//					while ((line = reader.readLine()) != null) {
//						int sep = line.indexOf('\t');	
//						idMap.put(line.substring(sep), line.substring(0, sep));
//					}
//					
//					p.println("Number of key value pairs in idmap = " + idMap.size());
//					p.close();
//					reader.close();	
//				}				
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			
//		}
		
		public void map(LongWritable key, Text values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			String[] tokens = values.toString().split("\t");
			try {
				String u_def = tokens[0] + "\t" + "entity";
				String v_def = tokens[2] + "\t" + tokens[1];
				String srcId = idMap.get(u_def);
				String dstId = idMap.get(v_def);
				outValue.set(srcId + "\t" + dstId);
				output.collect(outKey, outValue);
			} catch (ArrayIndexOutOfBoundsException e) {
			}			
		}

	}
}
