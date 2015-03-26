/********************************************
*Driver
*DriverSort
********************************************/
import java.net.URI;
 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;


public class Driver2 extends Configured implements Tool {
 
  @Override
	public int run(String[] args) throws Exception {
 
 
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		job.setJobName("sort");
 
		job.setJarByClass(Driver2.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map2.class);
		job.setReducerClass(Reduce2.class);
				
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		 
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),new Driver2(), args);
		System.exit(exitCode);
		
	}
}
