package canopy;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

import commom.*;

public class Canopy {

	public static  class textToVectorMapper extends Mapper<LongWritable,Text,LongWritable,VectorWritable>{
		
		public void map(LongWritable key,Text value,Context cox) throws IOException, InterruptedException{
			String [] str=value.toString().split(",");
			int leng=str.length;
			Vector vector=new Vector();
			for(int i=0;i<leng;i++){
			//	System.out.println(str[i]);
				vector.add(Double.parseDouble(str[i]));
			}
			VectorWritable vw=new VectorWritable(vector);
		//	System.out.println(vw.getVector().size());
			cox.write(key, vw);
		}
		
	}
	public static class textToVectorReducer extends Reducer<LongWritable,VectorWritable,LongWritable,VectorWritable>{
		public void reduce(LongWritable key,Iterable<VectorWritable> values,Context cox) throws IOException, InterruptedException{
			for(VectorWritable vw : values){
			//	System.out.println(vw.getVector().size());
				//for(int i=0;i<vw.getVector().size();i++)
			//		System.out.print(vw.getVector().getQuick(i));
			//	System.out.println();
				cox.write(key, vw);
			}
			
		}
	}
	
	public static class canopyMapper extends Mapper<LongWritable,VectorWritable,LongWritable,VectorWritable>{
		public Operate operat;
		public Collection<Cluster> clusters=Lists.newArrayList();
		//public Vector vector;
		public void setup(Context cox) throws IOException, InterruptedException{

			super.setup(cox);
			double T1,T2;
			T1=Double.parseDouble(cox.getConfiguration().get("T1"));
			T2=Double.parseDouble(cox.getConfiguration().get("T2"));
			operat=new Operate(T1,T2);
			
		}
		public void map(LongWritable key,VectorWritable value,Context cox){
		//	vector=new Vector();
		//	vector=value.getVector();
		//	for(int i=0;i<vector.size();i++)
			//	System.out.print(vector.getQuick(i));
		//	System.out.println();
			//System.out.println("center");
			operat.addPoint(clusters,value.getVector());
			//System.out.println("size"+clusters.size());
		}
		public void cleanup(Context cox) throws IOException, InterruptedException{
			for(Cluster cluster:clusters){
			//	System.out.println("11");
				cluster.computeParameters();
			
			 cox.write(new LongWritable(1),new VectorWritable(cluster.center));
				
			}
			super.cleanup(cox);
		}
	}
	public static class canopyReducer extends Reducer<LongWritable,VectorWritable,LongWritable,ClusterWritable>{
		
	//	public Vector vector;
		public Collection<Cluster> clusters=Lists.newArrayList();
		public Operate operate;
		public void setup(Context cox) throws IOException, InterruptedException{
			super.setup(cox);
			double T1,T2;
			T1=Double.parseDouble(cox.getConfiguration().get("T1"));
			T2=Double.parseDouble(cox.getConfiguration().get("T2"));
			operate=new Operate(T1,T2);
		}
		public void reduce(LongWritable key,Iterable<VectorWritable> values,Context cox) throws IOException, InterruptedException{
			for(VectorWritable vw:values){
			//vector=vw.getVector();
	//		System.out.println("1");
			operate.addPoint(clusters, vw.getVector());
			}
			for(Cluster cluster:clusters){
				cluster.computeParameters();
		//		System.out.println(cluster.id);
				System.out.println(cluster.numVector);
				for(int i=0;i<cluster.center.size();i++){
					System.out.println(cluster.center.getQuick(i));
				}
					System.out.println();
				cox.write(new LongWritable(cluster.id), new ClusterWritable(cluster));
			}
		}
		
	}
}
