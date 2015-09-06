import java.io.IOException;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;


public  class Reducer1 extends Reducer<Text, Text, Text, DoubleWritable>
{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int min=32;
			int max=0;
			double minAd=0;
			double maxAd=0;
			int count=0;
			double d;
			
			for(Text t:values)
			{
				String s=t.toString();
				String temp[]=s.split("\\__");
				if(min>Integer.parseInt(temp[0]))
				{
					min=Integer.parseInt(temp[0]);
					minAd=Double.parseDouble(temp[1]);
				}
				
				if(max<Integer.parseInt(temp[0]))
				{
					max=Integer.parseInt(temp[0]);
					maxAd=Double.parseDouble(temp[1]);
					
				}
			}
			
			d=(maxAd-minAd)/minAd; 
			context.write(key,new DoubleWritable(d));
			
		}
}