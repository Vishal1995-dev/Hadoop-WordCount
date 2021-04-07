package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>
{
	@Override
	protected void reduce(Text word, Iterable<IntWritable> it,
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		int total=0;
		for(IntWritable iw:it)
		{
			total+=iw.get();
		}
		
		context.write(word,new IntWritable(total));
	}
}
