/********************************************
*Mapper
*MapperSort
********************************************/
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;


import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
 
public class Map2 extends Mapper<LongWritable, Text, DoubleWritable, Text> {
 

	private DoubleWritable txtMapOutputKey = new DoubleWritable(0.0);
	private Text txtMapOutputValue = new Text("");
 
 
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 
		
		String row[] = value.toString().split("#");		
		double diff = Double.parseDouble(row[0]);	
		String stateMaxMin = row[1];
		
		txtMapOutputKey.set(diff);
		txtMapOutputValue.set(stateMaxMin);
		
		context.write(txtMapOutputKey, txtMapOutputValue);
		

		}
			
}