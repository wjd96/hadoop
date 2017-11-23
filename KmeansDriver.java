package Kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import Kmeans.Kmeans.*;
import canopy.Canopy;
import canopy.Canopy.textToVectorMapper;
import canopy.Canopy.textToVectorReducer;

import commom.ClusterWritable;
import commom.VectorWritable;

public class KmeansDriver {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		conf.set("time", "10");
		conf.set("limit","0");
		conf.set("flag", "1");
		conf.set("centerfile", "result/part-r-00000");
        Job job=new Job(conf,"textToVector");
		
		job.setJarByClass(Canopy.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setMapperClass(textToVectorMapper.class);
		job.setReducerClass(textToVectorReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("canopy/file1"));
		SequenceFileOutputFormat.setOutputPath(job,new Path("kmeansinput"));
		job.waitForCompletion(true);
		
		int time=0;
		System.out.println("time"+Integer.parseInt(conf.get("time")));
		while(time<Integer.parseInt(conf.get("time"))){
			conf.set("flag", "0");
			Job job11=new Job(conf,"Kmeans"+time);
			job11.setJarByClass(Kmeans.class);
			job11.setMapOutputKeyClass(LongWritable.class);
			job11.setMapOutputValueClass(ClusterWritable.class);
			job11.setOutputKeyClass(LongWritable.class);
			job11.setOutputValueClass(ClusterWritable.class);
			job11.setMapperClass(kmeansMapper.class);
			job11.setNumReduceTasks(0);
		//	job.setReducerClass(textToVectorReducer.class);
			System.out.println("a");
			job11.setOutputFormatClass(SequenceFileOutputFormat.class);
			job11.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.addInputPath(job11, new Path("kmeansinput/part-r-00000"));
			SequenceFileOutputFormat.setOutputPath(job11,new Path("result/"+time));
			conf.set("centerfile", "result/"+time+"/part-m-00000");
			System.out.println("b");
			job11.waitForCompletion(true);
			System.out.println("c");
			time++;
			
		}
		

	}

}
