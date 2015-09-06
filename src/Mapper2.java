import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Mapper2 extends Mapper<LongWritable,Text, Text, DoubleWritable>
{
	
	public void map(LongWritable key, Text value, Context context)
	{
		String line=value.toString();
		String keyValueArray[]=line.split("\\t");
		String temp[]=keyValueArray[0].split("____");
		Text op= new Text(temp[0]);
		double d=Double.parseDouble(keyValueArray[1]);
		try 
		{
			context.write(op,new DoubleWritable(d));
		} 
		catch (IOException e) 
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
	
	