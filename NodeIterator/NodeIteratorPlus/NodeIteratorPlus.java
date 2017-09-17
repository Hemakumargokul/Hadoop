import java.io.*;
import java.util.Map;
import java.net.URI;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus	;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.util.*;
public class NodeIteratorPlus
{
	// Mapper class for the Round 1 
	public static class CountTriangleMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		private Set<String> stopWords;
		private Path [] localFile= new Path[1];
  		private FileSystem fs;
  		private Map<String,String> order;
  		private Map<String,String> check;
  		private Map<String,ArrayList> neighbours;
  		
  	// setup for the Round 1 	
    protected void setup(Context context) throws IOException, InterruptedException {
        try{	
        		//read the input file and create order file 
        		check=new HashMap<String,String>();
        		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        		for(Path eachPath : localFiles){
        			String filename=eachPath.getName().toString().trim();
        			if(filename.equalsIgnoreCase("order.dat"))
        			readFile(filename);
        			
        		}

        		 

	        } catch(IOException ex) {

	            System.err.println("Exception in mapper setup: " + ex.getMessage());

	        }
    }

    private void readFile(String filePath) {

	        try{

	            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
	            //Read the order file and store it in hashmap for lookup
	            String next = null;
	             order= new HashMap<String,String>();
	            while((next = bufferedReader.readLine()) != null) {

	        		String[] strList = next.split("\\s+"); 
	        		 
	        		order.put(strList[0],strList[1]);	
	                


	            }

	        } catch(IOException ex) {

	            System.err.println("Exception while reading stop words file: " + ex.getMessage());

	        }

	    }
// Map for the Round 1
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    	{
	    		String str = value.toString();
			String[] strList = str.split("\\s+");
			 	
        
    			HashMap<String,String> h=new HashMap<String,String>(); 
			
				double d1=Double.parseDouble(order.get(strList[0]));
				
				double d2=0.0;
				try{
				
					d2=Double.parseDouble(order.get(strList[1]));
				}catch(Exception e)
				{

				}
				//check if the order of neighbor is greater only then emit the edge for processing 
				if(d2>d1){

					context.write(new Text(strList[0]), new Text(strList[1]));


								
				}
    			
    		}
  	}
  
  	// Reducer class for the Round 1 
  	public static class CountTriangleReducer extends Reducer<Text, Text, Text, Text>
	{

		// Get the start time of the Reducer task
		protected void setup(Context context) throws IOException, InterruptedException {
    		long startTimeMs = System.currentTimeMillis( );
    		System.out.println("Reducer 1 Start time:"+startTimeMs );
    	}
    	// Get the End time of the Reducer task
    	protected void cleanup(Context context) throws IOException, InterruptedException {
    		long endTimeMs = System.currentTimeMillis( );
    		System.out.println("Reducer 1 End time:"+endTimeMs);
    	}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			ArrayList<String> a1=new ArrayList<String>(); 
			int count = 0;
			for (Text val : values)
				a1.add(val.toString());

				
			HashMap<String,String> h=new HashMap<String,String>(); 
			
					//Form the 2-paths of all combinations and store it in hashmap		
		    		String[] a2 = a1.toArray(new String[a1.size()+100]);

		    		for(int i=0;i<a1.size();i++)
		    		{
		    			for(int j=0;j<a1.size();j++)
		    			{
		    				StringBuffer s1 = new StringBuffer(a2[i]);
		    				s1.append("_");
		    				s1.append(a2[j]);
		    				StringBuffer s2 = new StringBuffer(a2[j]);
							s2.append("_");
		    				s2.append(a2[i]);

		    				
		    				if(i!=j && !h.containsKey(s1.toString()) && !h.containsKey(s2.toString()))
		    				{
		    					
		    					h.put(s1.toString(),"");
		    					h.put(s2.toString(),"");
		    					
		    					context.write(key, new Text(s1.toString()));
		    				}
		    			}
		    		}
		    		    	
			
	    	}
	}

	
//Triangle counting Map/Reduce Round 2

	public static class CountTriangle1Mapper extends Mapper<LongWritable, Text, Text, Text>
	{
			@Override
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    	{
	    		String str = value.toString();
			String[] strList = str.split("\\s+");
			 	
	    		context.write(new Text(strList[1]),new Text(strList[0]));
	    	}	
	}
	// Second Mapper in Round 2 to read the input file and emit the input edge as key and value as $
	public static class CountTriangle2Mapper extends Mapper<LongWritable, Text, Text, Text>
	{
			@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    	{
	    		String str = value.toString();
			String[] strList = str.split("\\s+");
			 	String s=strList[0]+"_"+strList[1];
			 	String s1=strList[1]+"_"+strList[0];
	    		context.write(new Text(s),new Text("$"));
	    		context.write(new Text(s1),new Text("$"));
	    	}	
	}
//Third Mapper to Group all the count values of a particular node
public static class CountTriangle3Mapper extends Mapper<LongWritable, Text, Text, Text>
	{
			@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    	{
	    		String str = value.toString();
			String[] strList = str.split("\\s+");
			 	//String s=strList[0]+"_"+strList[1];
	    		context.write(new Text(strList[0]),new Text(strList[1]));
	    	}	
	}

	//Reducer of Round 2 to check for a edge and emit the count
	public static class CountTriangle1Reducer extends Reducer<Text, Text, Text, Text>
	{
		protected void setup(Context context) throws IOException, InterruptedException {
    		long startTimeMs = System.currentTimeMillis( );
    		System.out.println("Reducer 2 Start time:"+startTimeMs );
    	}

    	protected void cleanup(Context context) throws IOException, InterruptedException {
    		long endTimeMs = System.currentTimeMillis( );
    		System.out.println("Reducer 2 End time:"+endTimeMs);
    	}
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int count = 0;
			HashMap<String,String> h=new HashMap<String,String>(); 
			for (Text val : values){
				h.put(val.toString(),"");
				count++;
			}
			if(h.containsKey("$") && count>1)
			{
				for(Map.Entry<String,String> m:h.entrySet()){
					if(!m.getKey().equalsIgnoreCase("$"))
				 	context.write(new Text(m.getKey()),new Text("1"));	
				}
						
			}
						
	    	}
	}

//Round 3 reducer to group all the count for each node 
public static class CountTriangle3Reducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			long count=0;
		    
			for (Text val : values)
				count++;
			String str=""+count;
			context.write(key, new Text(str));
			
				//context.write(key, new Text(val));	
		    	
			
	    	}
	}
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job j1 = new Job(conf);

		//DistributedCache.addCacheFile(new URI(args[2]), j1.getConfiguration());
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), j1.getConfiguration());
		DistributedCache.addCacheFile(new Path(args[3]).toUri(), j1.getConfiguration());
		j1.setJobName("job1");
		j1.setJarByClass(NodeIteratorPlus.class);

		//Mapper output key and value type
		j1.setMapOutputKeyClass(Text.class);
		j1.setMapOutputValueClass(Text.class);

		//Reducer output key and value type
		j1.setOutputKeyClass(Text.class);
		j1.setOutputValueClass(Text.class);

		//file input and output of the whole program
		j1.setInputFormatClass(TextInputFormat.class);
		j1.setOutputFormatClass(TextOutputFormat.class);
		
		//Set the mapper class
		j1.setMapperClass(CountTriangleMapper.class);

		//set the combiner class for custom combiner
		//j1.setCombinerClass(WordReducer.class);

		//Set the reducer class
		j1.setReducerClass(CountTriangleReducer.class);

		//set the number of reducer if it is zero means there is no reducer
		j1.setNumReduceTasks(15);

		FileOutputFormat.setOutputPath(j1, new Path(args[1]));	
		FileInputFormat.addInputPath(j1, new Path(args[0]));

		int code = j1.waitForCompletion(true) ? 0 : 1;
		if(code == 0)
		{
			Job j2 = new Job(conf);

		//DistributedCache.addCacheFile(new URI(args[2]), j1.getConfiguration());
		//DistributedCache.addCacheFile(new Path(args[2]).toUri(), j2.getConfiguration());
		//DistributedCache.addCacheFile(new Path(args[3]).toUri(), j2.getConfiguration());
		j2.setJobName("job2");
		j2.setJarByClass(NodeIteratorPlus.class);

		MultipleInputs.addInputPath(j2,new Path(args[1]),TextInputFormat.class,CountTriangle1Mapper.class);
 			MultipleInputs.addInputPath(j2,new Path(args[0]),TextInputFormat.class,CountTriangle2Mapper.class);
		//Mapper output key and value type
		//j2.setMapOutputKeyClass(Text.class);
		//j2.setMapOutputValueClass(Text.class);

		//Reducer output key and value type
		j2.setOutputKeyClass(Text.class);
		j2.setOutputValueClass(Text.class);

		//file input and output of the whole program
		//j2.setInputFormatClass(TextInputFormat.class);
		j2.setOutputFormatClass(TextOutputFormat.class);
		// Mapper class for the Round 1 (TextOutputFormat.class);
		
		//Set the mapper class
		//j2.setMapperClass(CountTriangle1Mapper.class);
		//j2.setMapperClass(CountTriangle1Mapper.class);

		//set the combiner class for custom combiner
		//j1.setCombinerClass(WordReducer.class);

		//Set the reducer class
		j2.setReducerClass(CountTriangle1Reducer.class);

		//set the number of reducer if it is zero means there is no reducer
		j2.setNumReduceTasks(5);

		FileOutputFormat.setOutputPath(j2, new Path(args[4]));	
		//FileInputFormat.addInputPath(j2, new Path(args[1]));

		int code1=j2.waitForCompletion(true)? 0 : 1;
		

		if(code1 == 0)
		{
			Job j3 = new Job(conf);

		//DistributedCache.addCacheFile(new URI(args[2]), j1.getConfiguration());
		//DistributedCache.addCacheFile(new Path(args[2]).toUri(), j2.getConfiguration());
		//DistributedCache.addCacheFile(new Path(args[3]).toUri(), j2.getConfiguration());
		j3.setJobName("job3");
		j3.setJarByClass(NodeIteratorPlus.class);

		FileInputFormat.addInputPath(j3, new Path(args[4]));
		//Mapper output key and value type
		//j2.setMapOutputKeyClass(Text.class);
		//j2.setMapOutputValueClass(Text.class);

		//Reducer output key and value type
		j3.setOutputKeyClass(Text.class);
		j3.setOutputValueClass(Text.class);

		//file input and output of the whole program
		//j2.setInputFormatClass(TextInputFormat.class);
		j3.setOutputFormatClass(TextOutputFormat.class);
		j3.setMapperClass(CountTriangle3Mapper.class);
		//Set the mapper class
		//j2.setMapperClass(CountTriangle1Mapper.class);
		//j2.setMapperClass(CountTriangle1Mapper.class);

		//set the combiner class for custom combiner
		//j1.setCombinerClass(WordReducer.class);

		//Set the reducer class
		j3.setReducerClass(CountTriangle3Reducer.class);

		//set the number of reducer if it is zero means there is no reducer
		j3.setNumReduceTasks(2);

		FileOutputFormat.setOutputPath(j3, new Path(args[5]));	
		//FileInputFormat.addInputPath(j2, new Path(args[1]));

		j3.waitForCompletion(true);
		}

	}
 	}
}
