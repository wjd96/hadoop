package newGraph;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
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

import graph.buildGraph;
import graph.buildGraph.Claim;
import graph.buildGraph.claimWritable;
import graph.buildGraph.pMap;
import graph.buildGraph.pReduce;
import newGraph.Graph3.zGraph;
import newGraph.nGraph.Sets;
import newGraph.nGraph.initMap;
import newGraph.nGraph.initReduce;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class Graph2 {
	public static class Sets implements Writable{
		public ArrayList<String> claims=new ArrayList<String>();
		public Sets(){}
		public Sets(String claim){
			this.claims.add(claim);
		}
		public Sets(ArrayList<String> claims){
			for(String str:claims){
				this.claims.add(str);
			}
		}
		public Sets(String[] claims){
			for(int i=0;i<claims.length;i++){
				this.claims.add(claims[i]);
			}
		}
		public Sets copy(){
			Sets sets=new Sets();
			for(int i=0;i<this.claims.size();i++){
				sets.claims.add(this.claims.get(i));
			}
			return sets;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			int size=arg0.readInt();
			ArrayList<String> temp=new ArrayList<String>();
			for(int i=0;i<size;i++){
				int s=arg0.readInt();
				byte[] Cbytes=new byte[s];
				arg0.readFully(Cbytes);
				temp.add(new String(Cbytes,"GBK"));
			}
			this.claims=temp;
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			arg0.writeInt(claims.size());
			for(int i=0;i<claims.size();i++){
				byte[]t=this.claims.get(i).getBytes("GBK");
				arg0.writeInt(t.length);
				arg0.write(t);
			}
		}
	}

	//去掉只出现一次的车险的car_mark

	public static class initMap extends Mapper<Text,claimWritable,Text,Text>{
		public void map(Text key,claimWritable value,Context cox) throws IOException, InterruptedException{
			Claim claim=value.claim;
			for(String str:claim.car_marks){
				cox.write(new Text(str), new Text(claim.claim_id));
			}
//			cox.write(new Text(), new Sets(value.claim.car_marks));
		}
	}
	public static class initReduce extends Reducer<Text,Text,Text,Sets>{
		public void reduce(Text key,Iterable<Text> values,Context cox) throws IOException, InterruptedException{
//			System.out.print(key.toString()+" ");
			Sets sets=new Sets();
//			String claimids;
			int count=0;
			for(Text tx:values){
				count++;
				sets.claims.add(tx.toString());
			}
			if(count>1){
				cox.write(new Text(), sets);
			}	
		}
	}
	//去掉重复的claim_id子集
	public static class preMap extends Mapper<Text,Sets,Text,Text>{
		public void map(Text key,Sets value,Context cox) throws IOException, InterruptedException{
			Sets sets=value.copy();
			Collections.sort(sets.claims);
			String clid="";
			for(String str:sets.claims){
				clid+=str;
			}
			for(String str:sets.claims){
				cox.write(new Text(clid),new Text(str));
			}
		}
	}
	public static class preReduce extends Reducer<Text,Text,Text,Sets>{
		public void reduce(Text key,Iterable<Text>values,Context cox) throws IOException, InterruptedException{
			Sets sets=new Sets();
			for(Text t:values){
				if(!sets.claims.contains(t.toString()))
				sets.claims.add(t.toString());
			}
			cox.write(new Text(), sets);
		}
	}
	
	public static class selectMap extends Mapper<Text,Sets,Text,Sets>{
		public long count=0;
		public void map(Text key,Sets value,Context cox) throws IOException, InterruptedException{

			Collections.sort(value.claims);
			cox.write(new Text(value.claims.get(0)),value);
			for(int i=1;i<value.claims.size();i++){
				cox.write(new Text(value.claims.get(i)), new Sets(value.claims.get(0)));
			}
		}
	}

	public static class selectReduce extends Reducer<Text,Sets,Text,Sets>{
		public void reduce(Text key,Iterable<Sets> values,Context cox) throws IOException, InterruptedException{
			ArrayList<String> claims=new ArrayList<String>();
			String claimid="";
			int count=0;
			for(Sets sets:values){
				count++;
				claims.addAll(sets.claims);
			}
			claims=new ArrayList<String>(new LinkedHashSet<String>(claims));
			if(count==1){
				if(claims.size()==1){
					cox.write(new Text(claims.get(0)),new Sets(key.toString()));
				}else{
					cox.write(key, new Sets(claims));
				}
			}else{
				cox.write(key, new Sets(claims));
			}
			
		}
	}
	
	public static class partitionMap extends Mapper<Text,Sets,Text,Sets>{
		public void map(Text key,Sets value,Context cox) throws IOException, InterruptedException{
//			System.out.print(key.toString()+" ");
			cox.write(key, value);
		}
	}
	public static class partitionReduce extends Reducer<Text,Sets,Text,Sets>{
//		public String url=null;
//		public SequenceFile.Writer writer=null;
		FileWriter fw=null;
		public void setup(Context cox) throws IOException, InterruptedException{
			super.setup(cox);
			Configuration conf=cox.getConfiguration();
			String url=conf.get("url");
			File f=new File(url);
			fw=new FileWriter(f,true);
//			FileSystem fs=FileSystem.get(conf);
//			Path path=new Path(url);
//			writer=SequenceFile.createWriter(fs, conf, path, Text.class, Sets.class);
		}
		public void reduce(Text key,Iterable<Sets> values,Context cox) throws IOException, InterruptedException{
			ArrayList<String> claims=new ArrayList<String>();
			Sets msets=new Sets();
//			Sets lsets=new Sets();
			int count=0;
			String claimid="";
			for(Sets set:values){
				count++;
				if(set.claims.size()>1){
					msets=set.copy();
				}
			}
			if(msets.claims.size()!=count&&msets.claims.size()!=0){
				cox.write(new Text(), msets);
			}else if(msets.claims.size()==count){		
				/***输出子连通图***/
				Text text=new Text();
				Sets set=msets.copy();
//				writer.append(text, set);	
				for(String str:msets.claims){
					fw.write(str);
					fw.write(",");
					System.out.print(str+" ");
				}
				fw.write("\r\n");
				fw.write("\r\n");
				System.out.println(set.claims.size());
				System.out.println();
			}
		}
		public void cleanup(Context cox) throws IOException, InterruptedException{
//			writer.close();
			fw.close();
			super.cleanup(cox);
		}
	}
	
	public static class process extends Mapper<Object,Text,Text,Sets>{
		public void map(Object key,Text value,Context cox) throws IOException, InterruptedException{
				String line=new String(value.getBytes(),0,value.getLength(),"GBK");
				System.out.println(line);
				String[] strs=line.toString().split(" ");
				ArrayList<String> s=new ArrayList<String>(Arrays.asList(strs));
	            cox.write(new Text(), new Sets(s));
		}
	}
	
	public static class process1Mapper extends Mapper<Text,Sets,Text,claimWritable>{/**构造新的claimWritable****/
		public void map(Text key,Sets value,Context cox) throws IOException, InterruptedException{
			Sets set=value.copy();
			String flag=set.claims.get(0);
			for(int i=0;i<set.claims.size();i++){
				Claim claim=new Claim(flag);
				cox.write(new Text(set.claims.get(i)),new claimWritable(claim));
			}
		}
	}
	public static class process2Map extends Mapper<Text,claimWritable,Text,claimWritable>{/****合并同一个claim*****/
		public void map(Text key,claimWritable value,Context cox) throws IOException, InterruptedException{
			cox.write(key, value);
		}
	}
	public static class process2Reduce extends Reducer<Text,claimWritable,Text,claimWritable>{
		public void reduce(Text key,Iterable<claimWritable> values,Context cox) throws IOException, InterruptedException{
			Claim pc=new Claim();
			String nkey=null;
			for(claimWritable cw:values){
				if(cw.claim.car_marks.size()!=0){
					pc=cw.claim.copy();
				}else{
					nkey=cw.claim.claim_id;
				}
			}
			if(nkey!=null)
			cox.write(new Text(nkey), new claimWritable(pc));
		}
	}
	public static class process3Map extends Mapper<Text ,claimWritable,Text,claimWritable>{/****合并同组的claim******/
		public void map(Text key,claimWritable value,Context cox) throws IOException, InterruptedException{
			cox.write(key, value);
		}
	}
	public static class process3Reduce extends Reducer<Text,claimWritable,Text,zGraph>{
		public void reduce(Text key,Iterable<claimWritable> values,Context cox) throws IOException, InterruptedException{
			zGraph zg=new zGraph();
			for(claimWritable cw:values){
				zg.claims.add(cw.claim.copy());
			}
			cox.write(new Text(),zg);
		}
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
			Configuration conf=new Configuration();
			conf.set("url","d:/1/2/result.txt");
//			
			Job job1 = new Job(conf, "Data Deduplication");   
	        job1.setJarByClass(Graph2.class);	   
	        job1.setMapperClass(initMap.class);	   
	        job1.setReducerClass(initReduce.class);	   
	        job1.setMapOutputKeyClass(Text.class);
	   		job1.setMapOutputValueClass(Text.class);
	   		job1.setOutputKeyClass(Text.class);
	   		job1.setOutputValueClass(Sets.class);
	   		job1.setInputFormatClass(SequenceFileInputFormat.class);
	   		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	   		FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/G5/part-r-00000")); 
	        FileOutputFormat.setOutputPath(job1, new Path("hdfs://localhost:9000/G6"));
	        job1.waitForCompletion(true);
////	               
//	        
	        Job job2 = new Job(conf, "Data Deduplication");			   
	        job2.setJarByClass(Graph2.class);   
	        job2.setMapperClass(preMap.class);	   
	        job2.setReducerClass(preReduce.class);	   	   
	        job2.setMapOutputKeyClass(Text.class);
	   		job2.setMapOutputValueClass(Text.class);
	   		job2.setOutputKeyClass(Text.class);
	   		job2.setOutputValueClass(Sets.class);
	   		job2.setInputFormatClass(SequenceFileInputFormat.class);
	   		job2.setOutputFormatClass(SequenceFileOutputFormat.class);	   
	   		FileInputFormat.addInputPath(job2, new Path("hdfs://localhost:9000/G6/part-r-00000")); 
	        FileOutputFormat.setOutputPath(job2, new Path("hdfs://localhost:9000/G7/G8"));
	        job2.waitForCompletion(true) ;
		

		int b=11;
		long filelen=0;
		do{
		    Job jobs = new Job(conf, "Data Deduplication");		   
	        jobs.setJarByClass(Graph2.class);	   
	        jobs.setMapperClass(selectMap.class);	   
	        jobs.setReducerClass(selectReduce.class);	   
	        jobs.setMapOutputKeyClass(Text.class);
	   		jobs.setMapOutputValueClass(Sets.class);
	   		jobs.setOutputKeyClass(Text.class);
	   		jobs.setOutputValueClass(Sets.class);
	   		jobs.setInputFormatClass(SequenceFileInputFormat.class);
	   		jobs.setOutputFormatClass(SequenceFileOutputFormat.class);	   
	   		FileInputFormat.addInputPath(jobs, new Path("hdfs://localhost:9000/G7/G"+Integer.toString(b)+"/part-r-00000")); 
	        FileOutputFormat.setOutputPath(jobs, new Path("hdfs://localhost:9000/G7/G"+Integer.toString(b+1)));
	        jobs.waitForCompletion(true);
	       
	       
	        Job jobp = new Job(conf, "Data ");			   
	        jobp.setJarByClass(Graph2.class);	   
	        jobp.setMapperClass(partitionMap.class);	   
	        jobp.setReducerClass(partitionReduce.class);
	        jobp.setMapOutputKeyClass(Text.class);
	   		jobp.setMapOutputValueClass(Sets.class);
	   		jobp.setOutputKeyClass(Text.class);
	   		jobp.setOutputValueClass(Sets.class);
	   		jobp.setInputFormatClass(SequenceFileInputFormat.class);
	   		jobp.setOutputFormatClass(SequenceFileOutputFormat.class);	   
	   	    FileInputFormat.addInputPath(jobp, new Path("hdfs://localhost:9000/G7/G"+Integer.toString(b+1)+"/part-r-00000")); 
	        FileOutputFormat.setOutputPath(jobp, new Path("hdfs://localhost:9000/G7/G"+Integer.toString(b+2)));
	        jobp.waitForCompletion(true);
	        
	        b+=2;
	        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000/G7/G"+Integer.toString(b)+"/part-r-00000"),conf);
	        FileStatus fs = hdfs.getFileStatus(new Path("hdfs://localhost:9000/G7/G"+Integer.toString(b)+"/part-r-00000")); 
	        filelen=fs.getLen();
	        System.out.println(filelen);
		}while(filelen>73);

	}

}
