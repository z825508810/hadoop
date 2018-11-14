package cn.tarena.WCCount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * shuffle ¹ý³Ì split input buffer map buffer spill (partition,sort,combinner),merge(partition,sort,combinner) 
 * @author Administrator
 *
 */
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text _key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long count = 0;
		for (LongWritable val : values) {
			count += 1;
		}
		//_key = new Text(_key.toString() + "\t" + count);
		context.write(_key, new LongWritable(count));
	}

}
