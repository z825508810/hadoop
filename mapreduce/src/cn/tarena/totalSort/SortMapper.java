package cn.tarena.totalSort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, NumTimes, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String[] strs = ivalue.toString().split(" ");
		for (String str : strs) {
			NumTimes times = new NumTimes();
			times.setNum(Integer.parseInt(str));
			times.setCount(1);
			context.write(times,new IntWritable(1));
		}
	}

}
