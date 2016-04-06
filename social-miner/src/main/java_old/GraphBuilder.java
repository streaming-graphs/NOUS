import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GraphBuilder {

	public static class VertexMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text Value, Context context)
				throws IOException, InterruptedException {
			String line = Value.toString();
			String tokens[] = line.split(" ");
			for (String t : tokens) {
				context.write(new Text(t), new IntWritable(1));
			}
		}
	}

	public static class VertexDegreeCounter extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count = count + value.get();
			}
			context.write(key, new IntWritable(count));
		}
	}

	public static class PartitionCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private Text outKey = new Text();
		private int numRowsInPartition = 0;
		private IntWritable partitionId = new IntWritable(0);
		private IntWritable outValue = new IntWritable(0);

		public void setup(Context context) {
			partitionId.set(context.getConfiguration().getInt(
					"mapred.task.partition", 0));
			return;
		}

		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {

			numRowsInPartition++;
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {

			outKey.set(partitionId.toString());
			outValue.set(numRowsInPartition);
			context.write(outKey, outValue);
		}
	}

	public static class VertexIdMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		// private Text outKey = new Text();
		// private Text outValue = new Text("");
		private long offset = 0;
		private long numRows = 0;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			int partition_id = conf.getInt("mapred.task.partition", 0);
			String key = "vertex_partition_offset_"
					+ Integer.toString(partition_id);
			String partitionOffset = conf.get(key);
			offset = Integer.valueOf(partitionOffset);
		}

		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {

			// String[] tokens = values.toString().split("\t");
			//
			// StringBuilder sbuf = new StringBuilder("");
			// sbuf.append(offset + numRows++)
			// .append("\t")
			// .append(tokens[0])
			// .append("\t")
			// .append(tokens[1]);
			// outKey.set(sbuf.toString());
			// context.write(outKey, outValue);
			String data = values.toString();
			int sep = data.indexOf('\t');
			context.write(new Text(data.substring(0, sep)),
					new Text(Long.toString(offset + ++numRows)));
		}

	}

	public static class EdgeMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private HashMap<String, String> idMap = new HashMap<String, String>();
		private Text emptyValue = new Text("");

		public void setup(Context context) {

			try {
				Integer partition_id = new Configuration().getInt(
						"mapred.task.partition", 0);
//				PrintWriter p = new PrintWriter(new FileWriter(
//						"/people/d3m432/tmp/EdgeMapper.log."
//								+ partition_id.toString()));

				String vertexSummaryPath = context.getConfiguration().get(
						"vertexSummaryPath");
				FileSystem fs = FileSystem.get(context.getConfiguration());
				FileStatus[] files = fs.listStatus(new Path(vertexSummaryPath));
				System.out.println("*** READING " + files.length + " files");

				for (FileStatus f : files) {
					if (f.getPath().getName().toString().contains("part") == false)
						continue;
//					p.println("Reading ids from = " + f.getPath().toString());
					System.out.println("Reading ids from = " + f.getPath().toString());
//					p.flush();
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(fs.open(f.getPath())));
					String line = null;
					while ((line = reader.readLine()) != null) {
						int sep = line.indexOf('\t');
						idMap.put(line.substring(0, sep), line.substring(sep));
					}

//					p.println("Number of key value pairs in idmap = "
//							+ idMap.size());
					System.out.println("Number of key value pairs in idmap = "
							+ idMap.size());

					reader.close();
				}
//				p.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {

			String tokens[] = values.toString().split(" ");
			String srcId = idMap.get(tokens[0]);
			String dstId = idMap.get(tokens[1]);
			context.write(new Text(srcId + "\t" + dstId), emptyValue);
		}
	}

	protected void runVertexMappers() throws IOException, InterruptedException,
			ClassNotFoundException {

		Job job = Job.getInstance();
		job.setJobName("VertexMapper");
		job.setJarByClass(GraphBuilder.class);
		job.setMapperClass(GraphBuilder.VertexMapper.class);
		job.setReducerClass(GraphBuilder.VertexDegreeCounter.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, rawDataPath);
		FileOutputFormat.setOutputPath(job, vertexDegreeOutput);

		job.waitForCompletion(true);
		return;
	}

	protected void runPartitionCounter() throws IOException, InterruptedException,
			ClassNotFoundException {

		Job job = Job.getInstance();
		job.setJobName("VertexCounter");
		job.setJarByClass(GraphBuilder.class);
		job.setMapperClass(GraphBuilder.PartitionCountMapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, vertexDegreeOutput);
		FileOutputFormat.setOutputPath(job, partitionCountOutput);

		job.waitForCompletion(true);
		return;
	}

	protected void getVertexCount() throws IOException, InterruptedException,
			ClassNotFoundException {

		FileStatus[] files = fs.listStatus(partitionCountOutput);
		long offset = 0;

		Job job = Job.getInstance();
		job.setJobName("VertexIdMapper");
		Configuration conf = job.getConfiguration();

		for (FileStatus f : files) {
			String line = "";
			if (f.getPath().getName().toString().contains("part") == false)
				continue;
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fs.open(f.getPath())));

			while ((line = reader.readLine()) != null) {
				String[] tokens = line.split("\t");
				int numVerticesInPartition = Integer.valueOf(tokens[1]);
				offset += numVerticesInPartition;
				conf.set("vertex_partition_offset_" + tokens[0],
						Long.toString(offset));
			}
		}

		System.out.println("Number of vertices = " + offset);

		job.setJarByClass(GraphBuilder.class);
		job.setMapperClass(GraphBuilder.VertexIdMapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, vertexDegreeOutput);
		FileOutputFormat.setOutputPath(job, vertexSummaryPath);
		job.waitForCompletion(true);
		return;
	}

	protected void writeGraph() throws IOException, InterruptedException,
			ClassNotFoundException {

		Job job = Job.getInstance();
		job.setJobName("GraphWriter");
		job.getConfiguration().set("vertexSummaryPath",
				vertexSummaryPath.getName());
		job.setJarByClass(GraphBuilder.class);
		job.setMapperClass(GraphBuilder.EdgeMapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, rawDataPath);
		FileOutputFormat.setOutputPath(job, graphDataPath);

		job.waitForCompletion(true);
		return;
	}

	private FileSystem fs;
	private Path vertexDegreeOutput;
	private Path partitionCountOutput;
	private Path vertexSummaryPath;
	private Path rawDataPath;
	private Path graphDataPath;

	public void run(String inputDirectory, String scratchDirectory) {

		try {
			fs = FileSystem.get(new Configuration());
			fs.delete(new Path(scratchDirectory), true);
			rawDataPath = new Path(inputDirectory);
			vertexDegreeOutput = new Path(scratchDirectory
					+ "/pass1_vertex_lists");
			partitionCountOutput = new Path(scratchDirectory
					+ "/pass2_partition_vertex_count");
			vertexSummaryPath = new Path(scratchDirectory
					+ "/pass3_vertex_summary");
			graphDataPath = new Path(scratchDirectory + "/graph");
			
			System.out.println("Running VertexMapper ...");
			runVertexMappers();
			System.out.println("Running VertexCounter ...");
			runPartitionCounter();
			getVertexCount();
			System.out.println("Writing output ...");
			writeGraph();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
