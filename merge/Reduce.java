/********************************************
*Reducer
*ReducerMerge
********************************************/

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

public class Reduce extends Reducer<Text,Text,Text,Text> {
	   private Text txtReduceOutputKey = new Text("");
	   private Text txtReduceOutputValue = new Text("");
	   DecimalFormat df = new DecimalFormat("0.000");

	   public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		   
		   double[] monAccuT = new double[12];
		   int[] monCount = new int[12];
		   String[] month = {"Jan","Feb","Mar","April","May","Jun","July","Aug","Sep","Oct","Nov","Dec"};
		   double maxT = -100;
		   double minT = 100;
//		   int maxMon = 0;
//		   int minMon = 0;
		   String maxMon_str = "";
		   String minMon_str = "";
		   
		   for(Text val : values) {
			   String[] entry = val.toString().split(",");
			   int mon = Integer.parseInt(entry[0]) - 1;
			   double t = Double.parseDouble(entry[1]);
			   monAccuT[mon] += t;
			   monCount[mon]++;
		   }
		   
		   for(int i = 0; i < 12; i++) {
			   double monAveT = monAccuT[i] / monCount[i];
			   if (monAveT > maxT) {
				   maxT = monAveT;
				   maxMon_str = month[i];
			   }
			   if (monAveT < minT) {
				   minT = monAveT;
				   minMon_str = month[i];
			   }
		   }
		   
		   String maxT_string = df.format(maxT);
		   String minT_string = df.format(minT);
		   
		   String stateMaxMin = key.toString() + " | max T = " + maxT_string + " | max Mon = " + maxMon_str + " | min T = " + minT_string + " | min Mon = " + minMon_str;
		   double diff = maxT - minT;
		   String diff_num = df.format(diff);
		   txtReduceOutputValue.set(stateMaxMin);
		   txtReduceOutputKey.set(diff_num + "#");
		   
		   context.write(txtReduceOutputKey, txtReduceOutputValue);
	   }
}