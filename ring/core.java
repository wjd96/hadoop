package newGraph;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import graph.buildGraph.Claim;
import graph.buildGraph.claimWritable;
import newGraph.Graph2.Sets;
import newGraph.Graph2.selectMap;
import newGraph.Graph2.selectReduce;
public class Graph4 {
	public static class Car_mark implements Writable,Comparable{
		public String car_mark;
		public String claim_id;
		public Car_mark(){}
		public Car_mark(String car_mark,String claim_id){
			this.car_mark=car_mark;
			this.claim_id=claim_id;
		}
		public Car_mark copy(){
			Car_mark cm=new Car_mark();
			cm.car_mark=this.car_mark;
			cm.claim_id=this.claim_id;
			return cm;
		}
		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			int s1=arg0.readInt();
			byte[] Cbytes1=new byte[s1];
			arg0.readFully(Cbytes1);
			this.car_mark=new String(Cbytes1,"GBK");
			int s2=arg0.readInt();
			byte[] Cbytes2=new byte[s2];
			arg0.readFully(Cbytes2);
			this.claim_id=new String(Cbytes2,"GBK");
		}
		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			byte[]t1=this.car_mark.getBytes("GBK");
			arg0.writeInt(t1.length);
			arg0.write(t1);
			byte[]t2=this.claim_id.getBytes("GBK");
			arg0.writeInt(t2.length);
			arg0.write(t2);
		}
		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			Car_mark cm=(Car_mark)o;
			return this.car_mark.compareTo(cm.car_mark);
		}
	}
	public static class Inpair implements WritableComparable{
		public String car_mark1;
		public String car_mark2;
		public Inpair(){}
		public Inpair(String car_mark1,String car_mark2){
			this.car_mark1=car_mark1;
			this.car_mark2=car_mark2;
		}
		public Car_mark copy(){
			Car_mark cm=new Car_mark();
			cm.car_mark=this.car_mark1;
			cm.claim_id=this.car_mark2;
			return cm;
		}
		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			int s1=arg0.readInt();
			byte[] Cbytes1=new byte[s1];
			arg0.readFully(Cbytes1);
			this.car_mark1=new String(Cbytes1,"GBK");
			int s2=arg0.readInt();
			byte[] Cbytes2=new byte[s2];
			arg0.readFully(Cbytes2);
			this.car_mark2=new String(Cbytes2,"GBK");
		}
		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			byte[]t1=this.car_mark1.getBytes("GBK");
			arg0.writeInt(t1.length);
			arg0.write(t1);
			byte[]t2=this.car_mark2.getBytes("GBK");
			arg0.writeInt(t2.length);
			arg0.write(t2);
		}
		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			Inpair pair=(Inpair)o;
			if(!this.car_mark1.equals(pair.car_mark1)){
				return this.car_mark1.compareTo(pair.car_mark1);
			}else if(!this.car_mark2.equals(pair.car_mark2)){
				return this.car_mark2.compareTo(pair.car_mark2);
			}else{
				return 0;
			}
		}
		@Override
		public boolean equals(Object o){
			Inpair pair=(Inpair)o;
			return this.car_mark1.equals(pair.car_mark1)&&this.car_mark2.equals(pair.car_mark2)||this.car_mark1.equals(pair.car_mark2)&&this.car_mark2.equals(pair.car_mark1);
		}
	}
	//去掉非车牌号（AAAAAA类型）
	public static class process1 extends Mapper<Text,claimWritable,Text,claimWritable>{
//		public FileWriter fw=null;
		public HashSet<String> car=new HashSet<String>();
		public InputStreamReader read=null;
		public BufferedReader br=null;
		public void setup(Context cox) throws IOException, InterruptedException{
			super.setup(cox);
			Configuration conf=cox.getConfiguration();
			String url=conf.get("process");
			File f=new File(url);
			read=new InputStreamReader(new FileInputStream(f));
			br=new BufferedReader(read);
//			fw=new FileWriter(f,true);
			String line=null;
			while((line=br.readLine())!=null){
				car.add(line);
			}
			for(String str:car)
				System.out.println(str);
		}
		public void map(Text key,claimWritable value,Context cox) throws IOException, InterruptedException{
			boolean flag=false;
			for(String str:value.claim.car_marks){
				if(car.contains(str)){
					flag=true;
					break;
				}
			}
			if(flag==false){
				cox.write(key, value);
			}
			
		}
		public void cleanup(Context cox) throws IOException, InterruptedException{
			read.close();
			br.close();
			super.cleanup(cox);
		}
	}
	//去掉重复的claim_id子集
	public static class process2m extends Mapper<Text,claimWritable,Text,Text>{
		public void map(Text key,claimWritable value,Context cox) throws IOException, InterruptedException{
			Collections.sort(value.claim.car_marks);
			String s="";
			for(String str:value.claim.car_marks){
				s+=str+",";
			}
			cox.write(new Text(s),key);
		}
	}
	public static class process2r extends Reducer<Text,Text,Text,claimWritable>{
		public void reduce(Text key,Iterable<Text> values,Context cox) throws IOException, InterruptedException{
			String claimid=null;
			for(Text t:values){
				claimid=t.toString();
				break;
			}
			String[] strs=key.toString().split(",");
			ArrayList<String> s=new ArrayList<String>(Arrays.asList(strs));
			Claim c=new Claim(claimid,s);
			cox.write(new Text(claimid),new claimWritable(c));
		}
	}
	//求闭环链路算法
	public static class process1Map extends Mapper<Text,claimWritable,Text,Car_mark>{
		public void map(Text key,claimWritable value,Context cox) throws IOException, InterruptedException{
			Collections.sort(value.claim.car_marks);
			for(int i=0;i<value.claim.car_marks.size();i++){
				for(int j=0;j<value.claim.car_marks.size();j++){
					if(i!=j)
					cox.write(new Text(value.claim.car_marks.get(i)),new Car_mark(value.claim.car_marks.get(j),key.toString()));
				}
			}
		}
	}
	public static class process1Reduce extends Reducer<Text,Car_mark,Inpair,Car_mark>{
		public void reduce(Text key,Iterable<Car_mark> values,Context cox) throws IOException, InterruptedException{
			ArrayList<Car_mark> cms=new ArrayList<Car_mark>();
			for(Car_mark cm:values){
				cms.add(cm.copy());
			}
			Collections.sort(cms);
			if(cms.size()>1)
			for(int i=0;i<cms.size();i++){
				for(int j=i+1;j<cms.size();j++){
					if(!cms.get(i).car_mark.equals(cms.get(j).car_mark)){
						if(cms.get(i).claim_id.equals(cms.get(j).claim_id)){
							cox.write(new Inpair(cms.get(i).car_mark,cms.get(j).car_mark),new Car_mark(key.toString(),cms.get(i).claim_id));
						}else{
							cox.write(new Inpair(cms.get(i).car_mark,cms.get(j).car_mark), new Car_mark(key.toString(),"null"));
						} 
					}
				}
			}
		}
	}
	public static class process2Map extends Mapper<Text,claimWritable,Inpair,Car_mark>{
		public void map(Text key,claimWritable value,Context cox) throws IOException, InterruptedException{
			Collections.sort(value.claim.car_marks);
			for(int i=0;i<value.claim.car_marks.size();i++){
				for(int j=i+1;j<value.claim.car_marks.size();j++){
						cox.write(new Inpair(value.claim.car_marks.get(i),value.claim.car_marks.get(j)),new Car_mark("null",key.toString()));
				}
			}
		}
	}
	public static class process2Reduce extends Reducer<Inpair,Car_mark,Inpair,Car_mark>{
		public void reduce(Inpair key,Iterable<Car_mark> values,Context cox) throws IOException, InterruptedException{
			for(Car_mark cm:values){
				cox.write(key, cm.copy());
			}
		}
	}
	public static class process3Map extends Mapper<Inpair,Car_mark,Inpair,Car_mark>{
		public void map(Inpair key,Car_mark value,Context cox) throws IOException, InterruptedException{
			cox.write(key, value);
		}
	}
	public static class process3Reduce extends Reducer<Inpair,Car_mark,Text,Text>{
		public FileWriter fw=null;
		public ArrayList<String> carmarks=new ArrayList<String>();
		public void setup(Context cox) throws IOException, InterruptedException{
			super.setup(cox);
			Configuration conf=cox.getConfiguration();
			String url=conf.get("url");
			File f=new File(url);
			fw=new FileWriter(f,true);
		}
		public void reduce(Inpair key,Iterable<Car_mark>values,Context cox) throws IOException, InterruptedException{
			ArrayList<Car_mark>cms=new ArrayList<Car_mark>();
			for(Car_mark cm:values){
				cms.add(cm.copy());
//				System.out.print(cm.car_mark+" ");
			}
//			System.out.println();
			for(int i=0;i<cms.size();i++){
				for(int j=i+1;j<cms.size();j++){
//					if()
					Car_mark cm1=cms.get(i);
					Car_mark cm2=cms.get(j);
					String cars="";
					if(!cm1.claim_id.equals(cm2.claim_id)){		
						if(cm1.car_mark.equals("null")&&cm2.car_mark.equals("null")){
							cars+=key.car_mark1+",";
							cars+=key.car_mark2;
							carmarks.add(cars);
						}else if(cm1.car_mark.equals("null")&&!cm2.car_mark.equals("null")){
							cars+=key.car_mark1+",";
							cars+=key.car_mark2+",";
							cars+=cm2.car_mark;
							carmarks.add(cars);
						}else if(!cm1.car_mark.equals("null")&&cm2.car_mark.equals("null")){
							cars+=key.car_mark1+",";
							cars+=key.car_mark2+",";
							cars+=cm1.car_mark;
							carmarks.add(cars);
						}
					}else if(cm1.claim_id.equals("null")&&cm2.claim_id.equals("null")){
						if(!cm1.car_mark.equals(cm2.car_mark)){
							cars+=key.car_mark1+",";
							cars+=key.car_mark2+",";
							cars+=cm1.car_mark+",";
							cars+=cm2.car_mark;
							carmarks.add(cars);
						}	
					}
				}
			}
			cox.write(new Text(), new Text());
		}
		public void cleanup(Context cox) throws IOException, InterruptedException{
//			writer.close();
			carmarks=new ArrayList<String>(new LinkedHashSet<String>(carmarks));
			for(String str:carmarks){
				fw.write(str);
				fw.write("\r\n");
			}
			fw.close();
			super.cleanup(cox);
		}
	}

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf=new Configuration();
		conf.set("url","d:/1/2/result1.txt");//最终输出的结果
		//conf.set("test", "d:/1/2/test.txt");
		conf.set("process", "d:/1/2/filter.txt");//设置过滤词文件路径
		
		Job jobp1 = new Job(conf, "filter words");		   
		 jobp1.setJarByClass(Graph4.class);	   
		 jobp1.setMapperClass(process1.class);	   
//		 job1.setReducerClass(process1Reduce.class);	   
		 jobp1.setMapOutputKeyClass(Text.class);
		 jobp1.setMapOutputValueClass(claimWritable.class);
		 jobp1.setOutputKeyClass(Text.class);
		 jobp1.setOutputValueClass(claimWritable.class);
		 jobp1.setInputFormatClass(SequenceFileInputFormat.class);
		 jobp1.setOutputFormatClass(SequenceFileOutputFormat.class);	   
		 FileInputFormat.addInputPath(jobp1, new Path("hdfs://localhost:9000/G4/part-r-00000")); 
		 FileOutputFormat.setOutputPath(jobp1, new Path("hdfs://localhost:9000/G5"));
		 jobp1.waitForCompletion(true);
		 
		 
		 Job jobp = new Job(conf, "filter replica");		   
		 jobp.setJarByClass(Graph4.class);	   
		 jobp.setMapperClass(process2m.class);	   
		 jobp.setReducerClass(process2r.class);	   
		 jobp.setMapOutputKeyClass(Text.class);
		 jobp.setMapOutputValueClass(Text.class);
		 jobp.setOutputKeyClass(Text.class);
		 jobp.setOutputValueClass(claimWritable.class);
		 jobp.setInputFormatClass(SequenceFileInputFormat.class);
		 jobp.setOutputFormatClass(SequenceFileOutputFormat.class);	   
		 FileInputFormat.addInputPath(jobp, new Path("hdfs://localhost:9000/G5/part-r-00000")); 
		 FileOutputFormat.setOutputPath(jobp, new Path("hdfs://localhost:9000/G6"));
		 jobp.waitForCompletion(true);
		
//		
		 Job job1 = new Job(conf, "process1");		   
		 job1.setJarByClass(Graph4.class);	   
		 job1.setMapperClass(process1Map.class);	   
		 job1.setReducerClass(process1Reduce.class);	   
		 job1.setMapOutputKeyClass(Text.class);
		 job1.setMapOutputValueClass(Car_mark.class);
		 job1.setOutputKeyClass(Inpair.class);
		 job1.setOutputValueClass(Car_mark.class);
		 job1.setInputFormatClass(SequenceFileInputFormat.class);
		 job1.setOutputFormatClass(SequenceFileOutputFormat.class);	   
		 FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/G6/part-r-00000")); 
		 FileOutputFormat.setOutputPath(job1, new Path("hdfs://localhost:9000/G7"));
		 job1.waitForCompletion(true);
		 
		 Job job2 = new Job(conf, "process2");		   
		 job2.setJarByClass(Graph4.class);	   
		 job2.setMapperClass(process2Map.class);	   
		 job2.setReducerClass(process2Reduce.class);	   
		 job2.setMapOutputKeyClass(Inpair.class);
		 job2.setMapOutputValueClass(Car_mark.class);
		 job2.setOutputKeyClass(Inpair.class);
		 job2.setOutputValueClass(Car_mark.class);
		 job2.setInputFormatClass(SequenceFileInputFormat.class);
		 job2.setOutputFormatClass(SequenceFileOutputFormat.class);	   
		 FileInputFormat.addInputPath(job2, new Path("hdfs://localhost:9000/G6/part-r-00000")); 
		 FileOutputFormat.setOutputPath(job2, new Path("hdfs://localhost:9000/G8"));
		 job2.waitForCompletion(true);
//		
		 Job job3 = new Job(conf, "process3");		   
		 job3.setJarByClass(Graph4.class);	   
		 job3.setMapperClass(process3Map.class);	   
		 job3.setReducerClass(process3Reduce.class);	   
		 job3.setMapOutputKeyClass(Inpair.class);
		 job3.setMapOutputValueClass(Car_mark.class);
		 job3.setOutputKeyClass(Text.class);
		 job3.setOutputValueClass(Text.class);
		 job3.setInputFormatClass(SequenceFileInputFormat.class);
		 job3.setOutputFormatClass(SequenceFileOutputFormat.class);	   
		 FileInputFormat.addInputPath(job3, new Path("hdfs://localhost:9000/G8/part-r-00000")); 
		 FileInputFormat.addInputPath(job3, new Path("hdfs://localhost:9000/G7/part-r-00000")); 
		 FileOutputFormat.setOutputPath(job3, new Path("hdfs://localhost:9000/G9"));
		 job3.waitForCompletion(true);
	}

}
