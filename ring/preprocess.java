
package graph;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;  

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import io.netty.handler.codec.http.HttpHeaders.Values;
import jdk.internal.org.objectweb.asm.tree.analysis.Value;


 
public class buildGraph {

	public static class Claim{
		public String claim_id;
		public ArrayList<String> car_marks=new ArrayList<String>();
		public Claim(String str){
			this.claim_id=str;
		}
		public Claim(String str,ArrayList<String>strs){
			this.car_marks=strs;
			this.claim_id=str;
		}
		public Claim(){}
		public Claim copy(){
			Claim claim=new Claim();
			claim.claim_id=this.claim_id;
			for(int i=0;i<this.car_marks.size();i++){
				claim.car_marks.add(this.car_marks.get(i));
			}
			return claim;
		}
	}
	public static class claimWritable implements Writable{
		public Claim claim;
		public claimWritable(){
			this.claim=new Claim();
		}
		public claimWritable(Claim claim){
			this.claim=claim;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			int size=arg0.readInt();
			byte[] bytes=new byte[size];
			arg0.readFully(bytes);
			this.claim.claim_id=new String(bytes,"GBK");
			int car_nums=arg0.readInt();
			ArrayList<String>temp=new ArrayList<String>();
			for(int i =0;i<car_nums;i++){
				int s=arg0.readInt();
				byte[] Cbytes=new byte[s];
				arg0.readFully(Cbytes);
				temp.add(new String(Cbytes,"GBK"));
			}
			this.claim.car_marks=temp;
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			byte[]temp=this.claim.claim_id.getBytes("GBK");
			arg0.writeInt(temp.length);
			arg0.write(temp);
			arg0.writeInt(this.claim.car_marks.size());
			for(int i=0;i<this.claim.car_marks.size();i++){
				byte[]t=this.claim.car_marks.get(i).getBytes("GBK");
				arg0.writeInt(t.length);
				arg0.write(t);
			}
		}
		public boolean equals(Object o){
			if(o instanceof claimWritable){
				claimWritable cw=(claimWritable)o;
				if(!this.claim.claim_id.equals(cw.claim.claim_id)){
					return false;
				}
				if(this.claim.car_marks.size()!=cw.claim.car_marks.size()){
					return false;
				}
				for(String str:this.claim.car_marks){
					if(!cw.claim.car_marks.contains(str)){
						return false;
					}
				}
				return true;
			}
			return false;
		}
		
	}
	//聚集每个claim_id下所有的car_mark
    public static class Map extends Mapper<Object,Text,Text,Text>{/////////

        public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
        	
        	String line=new String(value.getBytes(),0,value.getLength(),"GBK");

           String[] strs=line.toString().split(",");

            context.write(new Text(strs[0]), new Text(strs[1]));

        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,claimWritable>{

        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
        	
        	Claim claim=new Claim();
        	
        	claim.claim_id=key.toString();
        	
        	for (Text t : values){
//        		String tp=new String(t.getBytes(),0,t.getLength(),"GBK");
        		claim.car_marks.add(t.toString());
        	}
        	
        	int car_nums=claim.car_marks.size();
        	
        	if(car_nums>=2){
        		
                context.write(key, new claimWritable(claim));
        	}
        }
    }
    //去掉只出现一次的险的车
    public static class processMap extends Mapper<Text,claimWritable,Text,Text>{
    	public void map(Text key,claimWritable value,Context context) throws IOException, InterruptedException{
    		Claim claim=value.claim;
    		for(String carmark:claim.car_marks){
    			context.write(new Text(carmark),new Text( claim.claim_id));
    		}
    	}
    }
    public static class processReduce extends Reducer<Text,Text,Text,claimWritable>{
    	public HashMap<String,Claim> claims=new HashMap<String,Claim>();
    	public long count=0;
    	public void reduce(Text key,Iterable<Text>values,Context context){
    		ArrayList<String> claim_ids=new ArrayList<String>();
    		for(Text t:values){
    			claim_ids.add(t.toString());
    		}
    		if(claim_ids.size()>1){
//    			System.out.println(count++);
    			for(String str:claim_ids){
    				if(claims.containsKey(str)){
    					Claim claim=claims.get(str);
    					claim.car_marks.add(key.toString());
    				}else{
    					Claim claim=new Claim();
    					claim.claim_id=str;
    					claim.car_marks.add(key.toString());
    					claims.put(str, claim);
    				}
    			}
    		}
    	}
    	public void cleanup(Context cox) throws IOException, InterruptedException{
    		System.out.print(claims.size());
    		for(Entry<String, Claim> entry:claims.entrySet()){
    			cox.write(new Text(entry.getKey()), new claimWritable(entry.getValue()));
    		}
    		super.cleanup(cox);
    	}
    }
    
    //去掉只有一辆车的事故claim_id
    
    public static class pMap extends Mapper<Text,claimWritable,Text,claimWritable>{


      public void map(Text key,claimWritable value,Context context)throws IOException,InterruptedException{
    	  
    	  if(value.claim.car_marks.size()>=2){
    		  context.write(key, value);
    	  }
      	
      }
  }
  public static class pReduce extends Reducer<Text,claimWritable,Text,claimWritable>{

      public void reduce(Text key,Iterable<claimWritable> values,Context context)throws IOException,InterruptedException{
      	
    	  for(claimWritable value:values){
    		  context.write(key, value);
    	  }
      	
      }
  }
  
  
   

    public static void main(String[] args) throws Exception{
    	//input hdfs path: hdfs://localhost:9000/G1
        Configuration conf = new Configuration();    
        Job job1 = new Job(conf, "collect car_mark");
        job1.setJarByClass(buildGraph.class);
        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);
     	job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(claimWritable.class);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/G1"));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://localhost:9000/G2"));
		System.out.print("job1");
		job1.waitForCompletion(true) ;
		
		
		
		Job job2 = new Job(conf, "filter car_mark");
		job2.setJarByClass(buildGraph.class);
		job2.setMapperClass(processMap.class);
		job2.setReducerClass(processReduce.class);
      	job2.setMapOutputKeyClass(Text.class);
 		job2.setMapOutputValueClass(Text.class);
 		job2.setOutputKeyClass(Text.class);
 		job2.setOutputValueClass(claimWritable.class);
 		job2.setInputFormatClass(SequenceFileInputFormat.class);
 		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
 		FileInputFormat.addInputPath(job2, new Path("hdfs://localhost:9000/G2/part-r-00000")); 
 		FileOutputFormat.setOutputPath(job2, new Path("hdfs://localhost:9000/G3"));
 		System.out.print("job2");
 		job2.waitForCompletion(true) ;
////        
        
        

        Job job3 = new Job(conf, "filter claim_id");
        job3.setJarByClass(buildGraph.class);
        job3.setMapperClass(pMap.class);
        job3.setReducerClass(pReduce.class);
        job3.setMapOutputKeyClass(Text.class);
   		job3.setMapOutputValueClass(claimWritable.class);
   		job3.setOutputKeyClass(Text.class);
   		job3.setOutputValueClass(claimWritable.class);
   		job3.setInputFormatClass(SequenceFileInputFormat.class);
   		job3.setOutputFormatClass(SequenceFileOutputFormat.class);
   		FileInputFormat.addInputPath(job3, new Path("hdfs://localhost:9000/G3/part-r-00000")); 
        FileOutputFormat.setOutputPath(job3, new Path("hdfs://localhost:9000/G4"));
        System.out.print("job3");
        job3.waitForCompletion(true);
      

     }
}
