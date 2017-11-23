package commom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class VectorWritable implements Writable{
	
	Vector vector;
	public VectorWritable() {
		vector=new Vector();
	}
	
	public VectorWritable(Vector vector){
		this.vector=vector;
	}

	public Vector getVector(){
		return vector;
	}
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		Vector vectortemp=new Vector();
		int size=arg0.readInt();
		for(int i=0;i<size;i++){
			vectortemp.add( arg0.readDouble());
		}
		
		vector=vectortemp;
		
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(vector.size);
		for(int i=0;i<vector.size;i++){
			arg0.writeDouble(vector.getQuick(i));
		}
		
	}
	
	
	

}
