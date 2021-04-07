package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>
{
	private final IntWritable one=new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException 
	{
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		
		String line =value.toString();
		String words[]=line.split(" ");
		
		for(String word:words)
		{
			context.write(new Text(word),one);
		}
	}
}
