/********************************************
*Mapper
*MapperMerge
********************************************/
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class Map extends
  	Mapper<LongWritable, Text, Text, Text> {
 
	private static HashMap<String, String> DepartmentMap = new HashMap<String, String>();
	private BufferedReader brReader;
	private String strDeptName = "";
	private Text txtMapOutputKey = new Text("");
	private Text txtMapOutputValue = new Text("");
 
	enum MYCOUNTER {
		RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	}
 
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
 
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
 
		for (Path eachPath : cacheFilesLocal) {
			if (eachPath.getName().toString().trim().equals("WeatherStationLocations.csv")) {
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				loadDepartmentsHashMap(eachPath, context);
			}
		}
 
	}
 
	private void loadDepartmentsHashMap(Path filePath, Context context)
			throws IOException {
 
		String strLineRead = "";
		String id = "";
		String state = "";
 
		try {
			brReader = new BufferedReader(new FileReader(filePath.toString()));
 
			// Read each line, split and load to HashMap
			while ((strLineRead = brReader.readLine()) != null) {
				
				String[] row = strLineRead.split(",");
				id = row[0].replace("\"", "");
				int le = row.length;
				String country = row[le - 7];
				state = row[le - 6];
			
				if (country.equalsIgnoreCase("\"US\"") && !state.equals("\"\"")) {
					state = state.replace("\"", "");
					DepartmentMap.put(id,state);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
		} catch (IOException e) {
			context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
			e.printStackTrace();
		}finally {
			if (brReader != null) {
				brReader.close();
 
			}
 
		}
	}
 
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
 
		context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
		String state;
		
		String row[] = value.toString().split("\\s+");		
		String id = row[0];	
		if (id.substring(0,1).equals("0"))
			id = id.substring(1);

		if (!id.equalsIgnoreCase("STN---") && DepartmentMap.containsKey(id)) {
			state = DepartmentMap.get(id);
			if(state != null){
				txtMapOutputKey.set(state);
				String mon = row[2].substring(4,6);
				String t = row[3];
				String monT = mon + "," + t;
				txtMapOutputValue.set(monT);
				context.write(txtMapOutputKey, txtMapOutputValue);
			}
		}

	}
}