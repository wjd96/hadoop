package canopy;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.google.common.collect.Lists;

import canopy.Canopy.*;

import commom.*;

public class CanopyDriver {

	 public static class RM extends Mapper<Text,ClusterWritable,Text,Text>{  
	        public void map(Text key,ClusterWritable value,Context context)throws InterruptedException,IOException{  
	            String str=value.cluster.center.toString(); 
	            context.write(key, new Text(str));  
	        }  
	 }

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub\
		
		Configuration conf=new Configuration();
		conf.set("T1", "8");
		conf.set("T2", "4");
		Job job=new Job(conf,"textToVector");
		
		job.setJarByClass(Canopy.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setMapperClass(textToVectorMapper.class);
		job.setReducerClass(textToVectorReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("canopy/file"));
		//FileInputFormat.addInputPath(job, new Path("canopy/file1"));
		SequenceFileOutputFormat.setOutputPath(job,new Path("canopyinput"));
		job.waitForCompletion(true);
	
        Job job1=new Job(conf,"canopy");
		
		job1.setJarByClass(Canopy.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(VectorWritable.class);
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(ClusterWritable.class);
		job1.setMapperClass(canopyMapper.class);
		job1.setReducerClass(canopyReducer.class);
		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileInputFormat.setInputPaths(job1, "canopyinput/part-r-00000");
		SequenceFileOutputFormat.setOutputPath(job1,new Path("result"));
		job1.waitForCompletion(true);
		
		
		
		

	}


	

}
