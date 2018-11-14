package cn.tarena.totalSort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class NumTimes implements WritableComparable<NumTimes> {
	private int num;
	private int count;

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.num = arg0.readInt();
		this.count = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(num);
		arg0.writeInt(count);
	}

	@Override
	public int compareTo(NumTimes o) {
		return -this.num + o.num;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "NumTimes [num=" + num + ", count=" + count + "]";
	}

}
