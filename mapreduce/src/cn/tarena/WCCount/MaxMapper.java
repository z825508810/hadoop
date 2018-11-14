package cn.tarena.WCCount;

import java.io.IOException;

import org.apache.commons.io.output.NullWriter;
import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 求最大值
 * 
 * @author Administrator
 *
 */
public class MaxMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
	int max = Integer.MIN_VALUE;

	public void map(IntWritable ikey, NullWritable ivalue, Context context) throws IOException, InterruptedException {
		int val = Integer.parseInt(ikey.toString());
		max = val > max ? val : max;
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		context.write(new IntWritable(max), null);
	}
}
