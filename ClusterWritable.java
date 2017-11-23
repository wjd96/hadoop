package commom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ClusterWritable implements Writable{
	
	public Cluster cluster;


	public ClusterWritable(){
		
		cluster=new Cluster();
		
	}
	
	public ClusterWritable(Cluster cluster){
		
		this.cluster=cluster;
	}
	

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		Cluster clustertemp=new Cluster();
		clustertemp.id=arg0.readInt();
		clustertemp.numVector=arg0.readInt();
	//	VectorWritable vn1=new VectorWritable(clustertemp.S1);
	//	vn1.readFields(arg0);
	//	VectorWritable vn=new VectorWritable(clustertemp.center);
	//	vn.readFields(arg0);
	//	cluster=clustertemp;
		Vector vectortemp=new Vector();
		int size=arg0.readInt();
		for(int i=0;i<size;i++){
			vectortemp.add(arg0.readDouble());
		}	
		clustertemp.center=vectortemp;
		Vector vectortemp1=new Vector();
		int size1=arg0.readInt();
		for(int i=0;i<size1;i++){
			vectortemp1.add(arg0.readDouble());
		}
		
		clustertemp.S1=vectortemp1;
		
		Vector vectortemp2=new Vector();
		int size2=arg0.readInt();
		for(int i=0;i<size2;i++){
			vectortemp2.add(arg0.readDouble());
		}
		
		clustertemp.oldcenter=vectortemp2;
		cluster=clustertemp;
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(cluster.id);
		arg0.writeInt(cluster.numVector);
		//VectorWritable vn1=new VectorWritable(cluster.S1);
		//vn1.write(arg0);
		//VectorWritable vn=new VectorWritable(cluster.center);
		//vn.write(arg0);
		arg0.writeInt(cluster.center.size);
		for(int i=0;i<cluster.center.size;i++)
			arg0.writeDouble(cluster.center.getQuick(i));
		arg0.writeInt(cluster.S1.size);
		for(int i=0;i<cluster.S1.size;i++)
			arg0.writeDouble(cluster.S1.getQuick(i));
		
		arg0.writeInt(cluster.oldcenter.size);
		for(int i=0;i<cluster.oldcenter.size;i++)
			arg0.writeDouble(cluster.oldcenter.getQuick(i));
	}

}
