package cn.tarena.score;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ScorePatitioner extends Partitioner<Text, Student> {

	@Override
	public int getPartition(Text arg0, Student arg1, int arg2) {

		return arg1.getMonth() - 1;
	}

}
