package decisionTree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class VectorWritable implements Writable {
	public Vector vector=new Vector();

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(vector.vector.size());
		for(Map.Entry<String, String> entry:vector.vector.entrySet()){
			out.writeChars(entry.getKey());
			out.writeChars(entry.getValue());
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		Vector temp=new Vector();
		int size=in.readInt();
		for(int i=0;i<size;i++){
			temp.vector.put(in.readUTF(), in.readUTF());
		}
		vector=temp;
	}

}
