import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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

public class Mapper3 extends Mapper<LongWritable,Text,IntWritable,Text>
{
	
	public void map(LongWritable key, Text value, Context context)
	{
		String line=value.toString();
		String keyValueArray[]=line.split("\\t");
		double d=Double.parseDouble(keyValueArray[1]);
		DecimalFormat df = new DecimalFormat("##.#####");
		String s=df.format(d);
		Text op= new Text(keyValueArray[0]+','+s);
		IntWritable one = new IntWritable(1);
		try 
		{
			context.write(one,op);
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