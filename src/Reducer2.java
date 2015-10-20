import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public  class Reducer2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
		{
			double x_bar=0;
			String s="";
			int N=0;
			double totalDob=0;
			ArrayList<Double> al=new ArrayList<Double>();
			for(DoubleWritable d:values)
			{
				double x=d.get();
				al.add(x);
				N++;
				x_bar+=x;
			}
			if(N!=0 && N!=1)
			{
				x_bar=(double)x_bar/N;
				for(double dob:al)
				{
					dob=(dob-x_bar)*(dob-x_bar);
					totalDob+=dob;

				}
						
				totalDob=(double)totalDob/(N-1);
				double volatility=Math.sqrt(totalDob);
				if(volatility!=0)
					context.write(key,new DoubleWritable(volatility));
			}
		}
}
