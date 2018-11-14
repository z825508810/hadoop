package cn.tarena.score;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ScoreMapper extends Mapper<LongWritable, Text, Text, Student> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		// ��ȡ�ļ�����split
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		// ��ȡ�ļ���
		String filename = fileSplit.getPath().getName();
		// ��ȡsubject
		String subject = filename.substring(0, filename.lastIndexOf("."));
		String[] infs = ivalue.toString().split(" ");
		Student stu = new Student();
		stu.setSubject(subject);
		stu.setMonth(Integer.parseInt(infs[0]));
		stu.setName(infs[1]);
		stu.setScore(Integer.parseInt(infs[2]));
		context.write(new Text(infs[1]), stu);
	}

}
