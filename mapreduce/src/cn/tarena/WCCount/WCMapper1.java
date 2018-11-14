package cn.tarena.WCCount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper1 extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		super.map(key, value, context);
		String[] words = value.toString().split(" ");
		for (int i = 0; i < words.length; i++) {
			context.write(new Text(words[i]), new LongWritable(1));
		}
	}
}
