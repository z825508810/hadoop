package cn.tarena.score;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReducer extends Reducer<Text, Student, Text, Text> {

	public void reduce(Text _key, Iterable<Student> values, Context context) throws IOException, InterruptedException {
		// process values
		int math = 0;
		int chinese = 0;
		int english = 0;
		for (Student val : values) {
			switch (val.getSubject()) {
			case "math":
				math = val.getScore();break;
			case "chinese":
				chinese = val.getScore();break;
			case "english":
				english = val.getScore();break;

			default:
				break;
			}
		}
		String val1 = math + "\t" + english + "\t" + chinese;
		context.write(_key, new Text(val1));
	}

}
