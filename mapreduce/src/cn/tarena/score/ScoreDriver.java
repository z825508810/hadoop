package cn.tarena.score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ScoreDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(cn.tarena.score.ScoreDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(ScoreMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(ScoreReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(3);
		job.setPartitionerClass(ScorePatitioner.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Student.class);
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://176.18.8.52:9000/scores1"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://176.18.8.52:9000/sout1"));

		if (!job.waitForCompletion(true))
			return;
	}

}
