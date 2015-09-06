import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text>
{
	
	public void map(LongWritable key, Text value, Context context)
	{
		 FileSplit fileSplit = (FileSplit)context.getInputSplit();
		 String fileName = fileSplit.getPath().getName();
		 String day="";
	     String temp[]=new String[7];
		 String date[]=new String[3];
		 String outputKey="";
		 String line = value.toString();
	     temp=line.split("\\,");
	     
	     
	     if(temp[0].contains("-"))
	     { 
	    	 date= temp[0].split("\\-");
	    	 outputKey=fileName+"____"+date[0]+","+date[1];
	    	 day=date[2]+"__"+temp[6];
	    	 Text op= new Text(outputKey);
	    	 Text va= new Text(day);
	    	 try 
	    	 {
	    		 context.write(op,va);
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
}
	
	
	
	
	
	


