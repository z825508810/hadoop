package cn.tarena.WCCount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String[] words = ivalue.toString().split(" ");
		for (int i = 0; i < words.length; i++) {
		    
			context.write(new Text(words[i]), new LongWritable(1));
		}
	}

}
