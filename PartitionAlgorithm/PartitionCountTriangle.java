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

public class PartitionCountTriangle
{
public static Map<Integer,HashSet> vertices;
public static HashSet<String> verticesList;
public static Map<String,String> order;
public static Map<String,String> edges;
public static int partitions=10; // Partition factor
public static class PartitionCountTriangleMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		protected void setup(Context context) throws IOException, InterruptedException {
       		 try{	
        		//read the input file and create order,vertices file and store it in HashMap
        			String filename=null;
        			Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        			for(Path eachPath : localFiles){
        			filename=eachPath.getName().toString().trim();
        			if(filename.equalsIgnoreCase("vertices.dat"))
        			{
        				//partition the vertices and store it
        				int flag=0;
        				
        				BufferedReader bufferedReader = new BufferedReader(new FileReader(filename));

	            		String next = null;
	             		vertices= new HashMap<Integer,HashSet>();
	             		verticesList=new HashSet<String>();
	             		int i=0;
	             		int hashId=0;
	            		int count=Integer.parseInt(bufferedReader.readLine());
	            		int counter=count/partitions;
	            		while((next = bufferedReader.readLine()) != null) {
	            			if(i==counter && counter!=0)
	            			{
	            				vertices.put(hashId,verticesList);
	            				verticesList=new HashSet<String>();
	            				i=0;
	            				hashId++;

	            			}
	            			verticesList.add(next);
	            				
	        				i++;
	            		}

	            		vertices.put(hashId,verticesList);	
        			}
        			else if(filename.equalsIgnoreCase("order.dat"))
        				readFile(filename);
        			else if(filename.equalsIgnoreCase("input.dat"))
        				readFile1(filename);
        		}
        			
        		}
        		catch(Exception e)
        			{

        			}		
			}

			private void readFile(String filePath) {

	        try{

	            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));

	            String next = null;
	             order= new HashMap<String,String>();
	            while((next = bufferedReader.readLine()) != null) {

	        		String[] strList = next.split("\\s+"); 
	        		//System.out.println(strList[0]+" "+strList[1]);   
	        		order.put(strList[0],strList[1]);	
	                //stopWords.add(stopWord.toLowerCase());


	            }

	        } catch(IOException ex) {

	            System.err.println("Exception while reading stop words file: " + ex.getMessage());

	        }

	    }

	    private void readFile1(String filePath) {

	        try{

	            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));

	            String next = null;
	             edges= new HashMap<String,String>();
	            while((next = bufferedReader.readLine()) != null) {

	        		String[] strList = next.split("\\s+");
	        		String s= strList[0]+"_"+strList[1];
	        		String s1= strList[1]+"_"+strList[0];
	        		//System.out.println(strList[0]+" "+strList[1]);   
	        		edges.put(s,"");
	        		edges.put(s1,"");	
	                //stopWords.add(stopWord.toLowerCase());


	            }

	        } catch(IOException ex) {

	            System.err.println("Exception while reading stop words file: " + ex.getMessage());

	        }

	    }

//Round 1 Map - forming groups of 3 
			@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    	{
	    		String str = value.toString();
				String[] strList = str.split(" ");
				int i=-1,j=-1;
				for (Map.Entry<Integer, HashSet> entry : vertices.entrySet()) {
    						//System.out.println("Key = " + entry.getKey() );
    						HashSet<String> a1= entry.getValue();
    						if(a1.contains(strList[0]))
    							i=entry.getKey();

    						if(a1.contains(strList[1]))
    							j=entry.getKey();

    						
    						if(i!=-1 && j!=-1)
    							{
    							//System.out.println(i+"-and-"+j);	
    							for(int a=0;a<partitions;a++)
								{
								for(int b=a+1;b<partitions;b++)
								{
								for(int c=b+1;c<partitions;c++)
								{
									if(i==a || i ==b || i==c)
									{
										if(j==a || j ==b || j==c)
										{
											String s1=a+""+b+""+c;
											String s2=strList[0]+"_"+strList[1];
								//			System.out.println("Key = " +s1+"  "+s2 );
											context.write( new Text(s1), new Text(s2));	
										}								
									}
								}	
								}
								}
								}	
							}
							
			   
	    	}
}

// For each group of vartices apply NodeIterator++ algorithm 
public static class PartitionCountTriangleReducer extends Reducer<Text, Text, Text, Text>
	{
		protected void setup(Context context) throws IOException, InterruptedException {
    		long startTimeMs = System.currentTimeMillis( );
    		System.out.println("Reducer 1 Start time:"+startTimeMs );
    	}

    	protected void cleanup(Context context) throws IOException, InterruptedException {
    		long endTimeMs = System.currentTimeMillis( );
    		System.out.println("Reducer 1 End time:"+endTimeMs);
    	}
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			// Get the neighbors of the nodes in a group
			Map<String,ArrayList> neighbours=new HashMap<String,ArrayList>();
				String next = null;
        				for (Text val : values) {
        					next=val.toString();
	            			
	        				String[] strList = next.split("_"); 
	        				if(neighbours.containsKey(strList[0]))
	        				{
	        					ArrayList<String> a1=neighbours.get(strList[0]);
								a1.add(strList[1]);
								neighbours.put(strList[0],a1);
	        				}
	            			else
	            			{
	            				ArrayList<String> a1=new ArrayList<String>();
	            				a1.add(strList[1]);
	            				neighbours.put(strList[0],a1);
	            			}
	            		}

	            	HashMap<String,String> h=new HashMap<String,String>(); 
			

				for (Map.Entry<String, ArrayList> entry : neighbours.entrySet()) {
		    		ArrayList<String> a1=neighbours.get(entry.getKey());
		    		String[] a2 = a1.toArray(new String[a1.size()+100]);
		    		
		    		try{

		    		double d1=Double.parseDouble(order.get(entry.getKey()));
		    		
		    		for(int i=0;i<a1.size();i++)
		    		{
		    			double d2=Double.parseDouble(order.get(a2[i]));	
		    			//order check
		    			if(d2>d1)
		    			{
		    				
		    			for(int j=0;j<a1.size();j++)
		    			{

		    				double d3=Double.parseDouble(order.get(a2[j]));
		    				if(d3>d2)
		    				{

		    				StringBuffer s1 = new StringBuffer(a2[i]);
		    				s1.append("_");
		    				s1.append(a2[j]);
		    				StringBuffer s2 = new StringBuffer(a2[j]);
							s2.append("_");
		    				s2.append(a2[i]);

		    				//duplicate a1_a2 a2_a1 check
		    				if(i!=j && !h.containsKey(s1.toString()) && !h.containsKey(s2.toString()))
		    				{	
		    					if(edges.containsKey(s1.toString()) || edges.containsKey(s2.toString())){
		    					
		    					h.put(s1.toString(),"");
		    					h.put(s2.toString(),"");
		    					int value=1;
		    					int ivalue=99999,jvalue=99999,nodevalue=99999;
		    					//vertices value calculation-partions Id
		    					for (Map.Entry<Integer, HashSet> entryV : vertices.entrySet()) {
		    						HashSet<String> list=entryV.getValue();
		    						
		    						if(list.contains(a2[i]))
		    							ivalue=entryV.getKey();
		    						if(list.contains(a2[j]))
		    							jvalue=entryV.getKey();
		    						if(list.contains(entry.getKey()))
		    							nodevalue=entryV.getKey();		

		    					}
		    					//Normalize the ouput 
		    					if(ivalue==jvalue)
		    					{
		    						if(jvalue==nodevalue)
		    						{
		    							int val1=nodevalue;
		    							int val2=partitions-val1-1;
		    							value=((val1*(val1-1))/2)+(val1*val2)+((val2*(val2-1))/2);
		    						}	
		    							
		    					} 
		    					else if((ivalue==jvalue) || (ivalue == nodevalue) || (jvalue == nodevalue))
		    						value=partitions-2;
		    					double v=(double)1/value;
		    					context.write(new Text(entry.getKey()), new Text(String.valueOf(v)));	
		    					}
		    					
		    					
		    				}
		    				}
		    			}
		    		}
		    		}
		    	}
		    	catch(Exception e){}	    	

		    	}

			
	    	}
	}
//Round 3 for grouping
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

	public static class CountTriangle3Reducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			double count=0;
		    
			for (Text val : values)
				count=count+Double.parseDouble(val.toString());
			int val=(int)count;
			String str=""+val;
			context.write(key, new Text(str));
			
				//context.write(key, new Text(val));	
		    	
			
	    	}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job j1 = new Job(conf);

		//DistributedCache.addCacheFile(new URI(args[2]), j1.getConfiguration());
		//DistributedCache.addCacheFile(new Path(args[2]).toUri(), j1.getConfiguration());
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), j1.getConfiguration());
		DistributedCache.addCacheFile(new Path(args[3]).toUri(), j1.getConfiguration());
		DistributedCache.addCacheFile(new Path(args[4]).toUri(), j1.getConfiguration());
		j1.setJobName("job1");
		j1.setJarByClass(PartitionCountTriangle.class);

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
		j1.setMapperClass(PartitionCountTriangleMapper.class);

		//set the combiner class for custom combiner
		//j1.setCombinerClass(WordReducer.class);

		//Set the reducer class
		j1.setReducerClass(PartitionCountTriangleReducer.class);

		//set the number of reducer if it is zero means there is no reducer
		j1.setNumReduceTasks(5);

		FileOutputFormat.setOutputPath(j1, new Path(args[1]));	
		FileInputFormat.addInputPath(j1, new Path(args[0]));

		int code = j1.waitForCompletion(true) ? 0 : 1;
		if(code == 0)
		{
			Job j3 = new Job(conf);

		//DistributedCache.addCacheFile(new URI(args[2]), j1.getConfiguration());
		//DistributedCache.addCacheFile(new Path(args[2]).toUri(), j2.getConfiguration());
		//DistributedCache.addCacheFile(new Path(args[3]).toUri(), j2.getConfiguration());
		j3.setJobName("job3");
		j3.setJarByClass(PartitionCountTriangle.class);

		FileInputFormat.addInputPath(j3, new Path(args[1]));
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