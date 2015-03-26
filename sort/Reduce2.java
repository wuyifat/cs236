import java.net.URI;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Reduce2 extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {

	   public void reduce(DoubleWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		   for (Text val : values)
			   context.write(key, val);
	   }
}