import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;


public class Main {

	public static void main(String[] args) throws IOException {
		
//		if (args.length < 1) {
//			System.out.println("USAGE: " + Main.class.getName() + 
//					" input-directory output-directory (default:input-dir.output_YYYY-MM-DD)");
//			System.exit(1);
//		}
		System.out.println("HADOOP_HOME: " + System.getenv("HADOOP_HOME"));
		String inputDir = "twitter"; //args[0];
		String outputDir = "twitter.output";
//		if (args.length == 1) {
//			DateFormat target = new SimpleDateFormat("yyyy-MM-dd_hh-mm");
//			Calendar now = new GregorianCalendar();
//			outputDir = inputDir + ".output_" + target.format(now.getTime());
//		}
//		else {
//			outputDir = args[1];
//		}
		System.out.println("Procesing input from: " + inputDir);
		System.out.println("Scratch directory: " + outputDir);
		GraphBuilder graphBuilder = new GraphBuilder();
		graphBuilder.run(inputDir, outputDir);
		return;
	}
}
