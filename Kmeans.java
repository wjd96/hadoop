package Kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.Lists;

import commom.*;

public class Kmeans {

	public static class kmeansMapper extends Mapper<LongWritable,VectorWritable,LongWritable,ClusterWritable>{
		public ArrayList<Cluster> clusters=Lists.newArrayList();
		public Operate operate=new Operate(0, 0);
		public void setup(Context cox) throws IOException, InterruptedException{
			super.setup(cox);
			Configuration conf=cox.getConfiguration();
			String path=conf.get("centerfile");
			FileSystem fs = FileSystem.get( conf);
			 SequenceFile.Reader reader=null;
			 reader=new SequenceFile.Reader(fs, new Path(path), conf); 
			 Writable key=(Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			 ClusterWritable cw=new ClusterWritable();
			 while(reader.next(key,cw)){
			//	 System.out.println("1");
				 Cluster cluster=new Cluster(cw.cluster.center,cw.cluster.id);
				 clusters.add(cluster);
			 }
		}
		public void map(LongWritable key,VectorWritable value,Context cox){
			int maxindex=0,i=0;
			double maxdistance=100000,distance;
			for(Cluster cluster:clusters){
				distance=operate.distanceMeasure(cluster.center, value.getVector());
			//	System.out.println("mark"+distance);
				if(distance<maxdistance){
					maxindex=i;
				    maxdistance=distance;
				}
				i++;
			}
			clusters.get(maxindex).addPoint(value.getVector());
		}
		public void cleanup(Context cox) throws IOException, InterruptedException{
			for(Cluster cluster:clusters){
				cluster.computeParameters();
				System.out.println("numvector"+cluster.numVector);
				System.out.println("center "+cluster.center.getQuick(0)+" "+cluster.center.getQuick(1));
				System.out.println("old "+cluster.oldcenter.getQuick(0)+" "+cluster.oldcenter.getQuick(1));
			//	System.out.println("2");
				double distance=operate.distanceMeasure(cluster.center, cluster.oldcenter);
				System.out.println(distance);
				if(distance>Double.parseDouble(cox.getConfiguration().get("limit")));
				cox.getConfiguration().set("flag", "1");
				cox.write(new LongWritable(cluster.id), new ClusterWritable(cluster));
				
			}
			//System.out.println("5");
			super.cleanup(cox);
		}	
	}
}
