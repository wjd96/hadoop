package commom;

import java.util.List;

public class Cluster  {
	
	public Vector center;
	
	public Vector S1;
	
	public int numVector;
	
	public int id;
	
	public Vector oldcenter;

	public Cluster(){
		
		this.center=new Vector();
		this.S1=new Vector();
		this.oldcenter=new Vector();
		numVector=1;
		this.S1=this.center.copy();
		this.oldcenter=this.center.copy();
		
	}
	public Cluster(Vector vector,int id){
		this.center=new Vector();
		this.S1=new Vector();
		this.oldcenter=new Vector();
		
		this.center=vector.copy();
		
		numVector=1;
		
		this.id=id;
		this.S1=this.center.copy();
		this.oldcenter=this.center.copy();
		
	}
	
	public void addPoint(Vector vector){
		System.out.println(id+" "+vector.getQuick(0)+" "+vector.getQuick(1));
		
    for(int i=0;i<S1.size;i++){
    	
    	double temp1= S1.getQuick(i);
    	double temp2= vector.getQuick(i);
    	temp1+=temp2;
    	S1.setQuick(i,temp1);
    	
    }
		numVector++;
	}
	

	public void computeParameters(){
	//	oldcenter=center;
	//	System.out.println(id+" "+S1.getQuick(0)+" "+S1.getQuick(1));
		if(numVector==0)
			return ;
		//System.out.println(id+" "+S1.getQuick(0)+" "+S1.getQuick(1)+" "+numVector);
		for(int i=0;i<S1.size;i++){
			
			double temp1=S1.getQuick(i);
			temp1/=numVector;
			center.setQuick(i,temp1);
			
		}
		
	}
	

}
