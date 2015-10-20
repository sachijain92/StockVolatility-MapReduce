import java.io.IOException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

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


public  class Reducer3 extends Reducer<IntWritable, Text,Text, DoubleWritable>
{
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
			int count=0;
			TreeSet<String> tm=new TreeSet<String>();
			for(Text t:values)
			{
				String s=t.toString();
				String temp[]=s.split(",");
				String s2=temp[1]+" "+temp[0];
				tm.add(s2);
			}
			int size = tm.size();
			Iterator<String> iterator = tm.iterator();
	
			while (iterator.hasNext()) 
			{
				if(count<10 || count>=(size-10))
				{
					String s=iterator.next();
					String temp[]=s.split(" ");
					double d=Double.parseDouble(temp[0]);
					if(count==0)
			           		context.write(new Text("---Minimum volatility---"),new DoubleWritable(10));
					if(count==size-10)
						context.write(new Text("---Maximum volatility---"),new DoubleWritable(10));
					String op[]=temp[1].split("\\.");
					context.write(new Text(op[0]),new DoubleWritable(d));
					count++;
				}
				else
				{
					iterator.next();
					count++;
				}
			}
		
	}
}
