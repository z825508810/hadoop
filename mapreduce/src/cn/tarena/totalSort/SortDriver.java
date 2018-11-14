package cn.tarena.totalSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(cn.tarena.totalSort.SortDriver.class);
		// TODO: specify a mapper  cn.tarena.totalSort.SortDriver
		job.setMapperClass(SortMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(SortReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(NumTimes.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapOutputKeyClass(NumTimes.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setPartitionerClass(SortPartition.class);
		job.setNumReduceTasks(3);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://176.18.8.53:9000/text/totalsort.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://176.18.8.53:9000/text/totalsort"));

		if (!job.waitForCompletion(true))
			return;
	}

}
