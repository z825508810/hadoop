package cn.tarena.score;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Student implements Writable {
	private int month;
	private int score;
	private String name;
	private String subject;

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getScore() {
		return score;
	}

	public void setScore(int score) {
		this.score = score;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.month = arg0.readInt();
		this.score = arg0.readInt();
		this.name = arg0.readUTF();
		this.subject = arg0.readUTF();

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(month);
		arg0.writeInt(score);
		arg0.writeUTF(name);
		arg0.writeUTF(subject);

	}

}
