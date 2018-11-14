package cn.tarena.totalSort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<NumTimes, IntWritable, NumTimes, NullWritable> {

	public void reduce(NumTimes _key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// process value
		int count = 0;

		for (IntWritable val : values) {
			count += 1;
		}
		_key.setCount(count);
		context.write(_key, null);
	}

}
