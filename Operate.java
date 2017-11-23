package commom;

import java.util.Collection;

public class Operate {


	double T1;
	
	double T2;
	
	int vectorid;
	
	public Operate(double T1,double T2){
		this.T1=T1;
		this.T2=T2;
		vectorid=0;
	}
	public double distanceMeasure(Vector v1,Vector v2){
		//System.out.println("3");
		
		double result=0;
		for(int i=0;i<v1.size;i++){
		//	System.out.println(v1.getQuick(i)+","+v2.getQuick(i));
			result+=Math.pow((v1.getQuick(i)-v2.getQuick(i)),2);
		}
		//System.out.println("result="+result);
		return Math.sqrt(result);
	}

	public void addPoint(Collection<Cluster> clusters,Vector vector){
		
	//System.out.println("dddd");
	//	Vector vectortemp=vector;

	boolean flag=false;
	if(clusters.size()==0){
		clusters.add(new Cluster(vector,vectorid++));
		return ;
	}
//	System.out.println(T1);
//	System.out.println(T2);
		for(Cluster cluster:clusters){
		
			//System.out.println("......");
		//	for(int i=0;i<cluster.center.size;i++)
			//	System.out.print("center"+cluster.center.getQuick(i)+"id"+cluster.id);
		//	System.out.println();
		double distance=distanceMeasure(cluster.center,vector);
	//	System.out.println(distance);
		if(distance<T1){
			cluster.addPoint(vector);	
		//	System.out.println("id"+cluster.id+" "+vector.getQuick(0)+" "+vector.getQuick(1));
		}
		if(distance<T2){
			flag=true;
		}		
		}
		if(flag==false)
			clusters.add(new Cluster(vector,vectorid++));
			
	}
	

}
