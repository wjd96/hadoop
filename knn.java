package KNN;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class knn {

	public static class Vector{
		String label;
		ArrayList<Double> set=new ArrayList<Double>();
	}
	public class VectorWritable implements Writable{
		Vector vector=new Vector();
		public VectorWritable(Vector vector){
			this.vector=vector;
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(vector.label);
			out.writeInt(vector.set.size());
			for(Double d:vector.set){
				out.writeDouble(d);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			Vector temp=new Vector();
			temp.label=in.readUTF();
			int total=in.readInt();
			for(int i=0;i<total;i++){
				vector.set.add(in.readDouble());
			}
		}	
	}
	public static class Pair implements Writable{
		String type;
		Double distance;
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(type);
			out.writeDouble(distance);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			type=in.readUTF();
			distance=in.readDouble();
		}
	}
	public class TMAP extends Mapper<LongWritable,Text,Text,VectorWritable>{
		public void map(LongWritable key,Text value,Context cox) throws IOException, InterruptedException{
			String str=value.toString();
			String[] tuple=str.split(" ");
			Vector vector=new Vector();
			vector.label=tuple[0];
			for(int i=1;i<tuple.length;i++){
				vector.set.add(Double.parseDouble(tuple[i]));
			}
			cox.write(new Text(tuple[0]),new VectorWritable(vector));
		}
	}
	public class TREDUCE extends Reducer<Text,VectorWritable,Text,VectorWritable>{
		public void redue(Text key,Iterable<VectorWritable> values,Context cox) throws IOException, InterruptedException{
			for(VectorWritable VW:values){
				cox.write(key, VW);
			}
		}
	}
	public static class TestMap extends Mapper<LongWritable,Text,Text,Pair>{
		public Integer index;
		public ArrayList<Vector> vectors;
		public ArrayList<Vector> Train;
		public void setup(Context cox) throws IOException, InterruptedException{
			super.setup(cox);
			index=0;
			vectors=new ArrayList<Vector>();
			Train=new ArrayList<Vector>();
			FileSystem fs=FileSystem.get(cox.getConfiguration()); 
			String str=cox.getConfiguration().get("Train");
		    Path path=new Path(str);  
		    FSDataInputStream fsd =((FileSystem) fs).open(path);  
		    BufferedReader bis = new BufferedReader(new InputStreamReader(fsd,"GBK")); 
		    String temp;
		    while ((temp = bis.readLine()) != null) {  
		    	  String[] strs=temp.split(" ");
		    	  System.out.println(temp);
		          Vector vector=new Vector();
		          vector.label=strs[0];
		          for(int i=1;i<strs.length;i++){
		        	  vector.set.add(Double.parseDouble(strs[i]));
		          }  
		          Train.add(vector);
		    }
		 //   bis.close();
		  //  fsd.close();
		   // fs.close();
		}
		public void map(LongWritable key,Text value,Context cox) throws IOException, InterruptedException{
			Vector vector =new Vector();
			String str=value.toString();
			System.out.println(str);
			String[] tuple=str.split(" ");
			for(int i=0;i<tuple.length;i++){
				vector.set.add(Double.parseDouble(tuple[i]));
			}
			vector.label=index.toString();
			for(Vector temp:Train){
				Pair pair=Distance(temp,vector);
				cox.write(new Text(vector.label),pair);
			}
			vectors.add(vector);
			index++;
		}
		public Pair Distance(Vector temp, Vector vector) {
			// TODO Auto-generated method stub
			Pair pair=new Pair();
			Double distance=(double) 0;
			for(int i=0;i<temp.set.size();i++){
				distance+=Math.pow(temp.set.get(i)-vector.set.get(i),2);
			}
			pair.type=temp.label;
			pair.distance=Math.sqrt(distance);
			return pair;
		}
		public void cleanup(Context cox) throws IOException, InterruptedException{
			FileSystem fs=FileSystem.get(cox.getConfiguration()); 
			String str=cox.getConfiguration().get("hash");
		    Path path=new Path(str);  
		 //   FileWriter fos = new FileWriter(str);  
	     //   BufferedWriter bw = new BufferedWriter(fos);
	        StringBuffer sbr=new StringBuffer();
	        FSDataOutputStream out = fs.create(path); 
			for(Vector temp:vectors){
				sbr.append(temp.label);
				for(Double d:temp.set){
					sbr.append(" "+d.toString());
				}
				sbr.append("\r\n");
			}
			System.out.println(sbr.toString());
			out.writeUTF(sbr.toString());;
		   // fos.close();
		 //   bw.close();
		  //  fs.close();
		    super.cleanup(cox);
		}
	}
	public static class TestReduce extends Reducer<Text,Pair,Text,Text>{
		public int K;
		public void setup(Context cox){
			K=cox.getConfiguration().getInt("K", 0);
		}
		public void reduce(Text key,Iterable<Pair> values,Context cox) throws IOException, InterruptedException{
			ArrayList<Pair> pairs=new ArrayList<Pair>();
			for(Pair pair:values){
				Pair temp=new Pair();
				temp.type=pair.type;
				temp.distance=pair.distance;
				pairs.add(temp);
			}
			String type=getType(pairs,K);
			cox.write(key,new Text(type));
		}

		private String getType(ArrayList<Pair> pairs,int k) {
			Collections.sort(pairs,new pComparator());
			Map<String,Integer> types=new HashMap<String,Integer>();
			for(int i=0;i<k;i++){
				if(!types.containsKey(pairs.get(i).type))
				    types.put(pairs.get(i).type,1);
				else
					types.put(pairs.get(i).type, types.get(pairs.get(i).type)+1);
			}
			List<Map.Entry<String,Integer>> list =new ArrayList<Map.Entry<String,Integer>>(types.entrySet());
		    Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
		        public int compare(Map.Entry<String, Integer> o1,
		            Map.Entry<String, Integer> o2) {
		              return (o2.getValue() - o1.getValue());
		            }
		        });
		    for(Map.Entry<String,Integer> entry:list){
		    	return entry.getKey();
		    }
			return null;
		}
		public class pComparator implements Comparator{ 
			public int compare(Object o1,Object o2) { 
			Pair e1=(Pair)o1; 
			Pair e2=(Pair)o2; 
			if(e1.distance>e2.distance) 
			return 1; 
			else 
			return -1; 
			} 	
		} 
	}

}

