import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StockVolatilityDriver 
{
	public static void main(String[] args) 
	{
		try 
		{
				// Create a new Job
			long start = new Date().getTime();
			Job job = Job.getInstance();
			Job job2=Job.getInstance();
	     		Job job3=Job.getInstance();
		     
			job.setJarByClass(StockVolatilityDriver.class);
			job2.setJarByClass(StockVolatilityDriver.class);
			job3.setJarByClass(StockVolatilityDriver.class);
			
			job.setMapperClass(Mapper1.class);		
			job.setReducerClass(Reducer1.class);
			job2.setMapperClass(Mapper2.class);
			job2.setReducerClass(Reducer2.class);
			job3.setMapperClass(Mapper3.class);
			job3.setReducerClass(Reducer3.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(DoubleWritable.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);
			job3.setMapOutputKeyClass(IntWritable.class);
			job3.setMapOutputValueClass(Text.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(DoubleWritable.class);
			
			
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			job3.setInputFormatClass(TextInputFormat.class);
			job3.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]+"hey"));
			FileInputFormat.addInputPath(job2, new Path(args[1]+"hey"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]+"output"));
			FileInputFormat.addInputPath(job3, new Path(args[1]+"output"));
			FileOutputFormat.setOutputPath(job3, new Path(args[1]));
			
			job.setJarByClass(StockVolatilityDriver.class);
			job.waitForCompletion(true);		
			job2.waitForCompletion(true);
			boolean status=job3.waitForCompletion(true);
			if (status == true) 
			{
				long end = new Date().getTime();
				System.out.println("\nJob took " + (end-start)/60000 + " minutes\n");
			}
			
			
			
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (ClassNotFoundException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (InterruptedException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

