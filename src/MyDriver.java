package in.olc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver 
{
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		String iPath="hdfs://localhost:9000/user/hduser/wordcount_input";
		String oPath="hdfs://localhost:9000/user/hduser/wordcount_output";
		
		Path inputPath=new Path(iPath);
		Path outputPath=new Path(oPath);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJarByClass(MyDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		// all link to mapper, reducer,driver program,
		// input path ,output path.

	}
}
