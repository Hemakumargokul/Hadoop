import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
public class NodeOrdering 
{
	//Round 1 read the input and emit edges
	public static class OrderReadMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    	{
				String str = value.toString();
			String[] strList = str.split("\\s+");

			   context.write( new Text(strList[0]), new Text(strList[1]));	
				
    		}
  	}
	//Round 2 
	public static class OrderMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    	{
				String str = value.toString();
			String[] strList = str.split("\\s+");

			   context.write( new Text(strList[1]), new Text(strList[0]));	
				
    		}
  	}
  	// Round 2
  	public static class OrderReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			long count=0;
		    
			//Break the tie of same degree by having a second count
			for (Text val : values){
			count++;
				
			context.write(new Text(val), new Text(key+"."+count));	
			}
			
	    	}
	}
	//Round 1 reducers to get the neighbor count for each node
	public static class OrderReadReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			long count=0;
		    
			for (Text val : values)
				count++;
			String str=""+count;
			context.write(key, new Text(str));
	    	}
	}
	
public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job j1 = new Job(conf);

		j1.setJobName("Wordcount job");
		j1.setJarByClass(NodeOrdering.class);

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
		j1.setMapperClass(OrderReadMapper.class);

		//set the combiner class for custom combiner
		//j1.setCombinerClass(WordReducer.class);

		//Set the reducer class
		j1.setReducerClass(OrderReadReducer.class);

		//set the number of reducer if it is zero means there is no reducer
		j1.setNumReduceTasks(2);

		FileOutputFormat.setOutputPath(j1, new Path(args[1]));	
		FileInputFormat.addInputPath(j1, new Path(args[0]));

		int code = j1.waitForCompletion(true) ? 0 : 1;
		if(code == 0)
		{
					
		Job j2 = new Job(conf, "Sorting Job");

		//j2.setJobName("Wordcount job");
		j2.setJarByClass(NodeOrdering.class);

		//Mapper output key and value type
		j2.setMapOutputKeyClass(Text.class);
		j2.setMapOutputValueClass(Text.class);

		//Reducer output key and value type
		j2.setOutputKeyClass(Text.class);
		j2.setOutputValueClass(Text.class);

		//file input and output of the whole program
		j2.setInputFormatClass(TextInputFormat.class);
		j2.setOutputFormatClass(TextOutputFormat.class);
		
		//Set the mapper class
		j2.setMapperClass(OrderMapper.class);

		//set the combiner class for custom combiner
		//j1.setCombinerClass(WordReducer.class);

		//Set the reducer class
		j2.setReducerClass(OrderReducer.class);

		//set the number of reducer if it is zero means there is no reducer
		j2.setNumReduceTasks(2);

		FileOutputFormat.setOutputPath(j2, new Path(args[2]));	
		FileInputFormat.addInputPath(j2, new Path(args[1]));
		j2.waitForCompletion(true);
		}


 	}
}
