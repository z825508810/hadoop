package cn.tarena.WCCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(cn.tarena.WCCount.WCDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(WCMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(WCReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setCombinerClass(WCReducer.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://176.18.8.52:9000/text/characters.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://176.18.8.52:9000/text/result2"));

		if (!job.waitForCompletion(true))
			return;
	}

}
