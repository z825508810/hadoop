package cn.tarena.totalSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class SortPartition extends org.apache.hadoop.mapreduce.Partitioner<NumTimes, IntWritable> {

	@Override
	public int getPartition(NumTimes arg0, IntWritable arg1, int arg2) {
		if (arg0.getNum() < 100) {
			return 0;
		} else if (arg0.getNum() < 1000) {
			return 1;
		}
		return 2;
	}

}
