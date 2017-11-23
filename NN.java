package NN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class NN {

	public class node{
		public int level;
	}
	public class inputNode{
		public double output;
	}
	public class midNode{
		public double input;
		public double output;
		public double a;
		public double b;
		public double u;
	}
	public class outNode{
		public double input;
		public double output;
		public double a;
		public double b;
		public double u;
	}
	public class wMatrix{
		public int fl;
		public int bl;
		public double matrix [][];
	}
	public static class Vector{
		public ArrayList<Double> code =new ArrayList<Double>();
		public ArrayList<Double> attr =new ArrayList<Double>();
	}
	public static Map<Integer,inputNode> inputlevel=new HashMap<Integer,inputNode>();
	public static Map<Integer,midNode> midlevel=new HashMap<Integer,midNode>();
	public static Map<Integer,outNode> outlevel=new HashMap<Integer,outNode>();
	public wMatrix wMatrix1=new wMatrix();
	public wMatrix wMatrix2=new wMatrix();
	public double l;
	public static int right,worry;
//	public static int input,mid,out;
	public void init(int inputn,int midn,int outn){
		l=0.1;
	//	input=inputn;
	//	mid=midn;
	//	out=outn;
		for(int i=0;i<inputn;i++){
			inputNode temp=new inputNode();
			inputlevel.put(i, temp);
		}
		for(int i=0;i<midn;i++){
			midNode temp=new midNode();
			temp.input=0;
			temp.u=0;
			midlevel.put(i,temp);
		}
		for(int i=0;i<outn;i++){
			outNode temp=new outNode();
			temp.input=0;
			temp.u=0;
			outlevel.put(i,temp);
		}
		wMatrix1.fl=0;
		wMatrix1.bl=1;
		wMatrix2.fl=1;
		wMatrix2.bl=2;
		wMatrix1.matrix=new double[inputn][midn];
		wMatrix2.matrix=new double[midn][outn];
		for(int i=0;i<inputn;i++){
			for(int j=0;j<midn;j++){
				wMatrix1.matrix[i][j]=Math.random();
			}
		}
		for(int i=0;i<midn;i++){
			for(int j=0;j<outn;j++){
				wMatrix2.matrix[i][j]=Math.random();
			}
		}
		for(int i=0;i<midn;i++){
			midlevel.get(i).b=Math.random();
		}
		for(int i=0;i<outn;i++){
			outlevel.get(i).b=Math.random();
		}
	}
	public void forword(ArrayList<Vector> vectors){
		for(Vector vector:vectors){
			for(int i=0;i<midlevel.size();i++){
				midNode temp=midlevel.get(i);
				temp.input=0;
				temp.u=0;
			//	midlevel.put(i,temp);
			}
			for(int i=0;i<outlevel.size();i++){
				outNode temp=outlevel.get(i);
				temp.input=0;
				temp.u=0;
			//	outlevel.put(i,temp);
			}
			
			action(vector);
		}
	}
	private void action(Vector vector) {
		// TODO Auto-generated method stub
		double sum=0;
		for(int i=0;i<vector.attr.size();i++){
			sum+=vector.attr.get(i);
		}
		for(int i=0;i<inputlevel.size();i++){
			inputNode in=new inputNode();
			in=inputlevel.get(i);
			in.output=vector.attr.get(i);
		//	System.out.print(in.output+" ");
		}
		//System.out.println();
		for(int i=0;i<inputlevel.size();i++){
			for(int j=0;j<midlevel.size();j++){
				midNode mn=midlevel.get(j);
				mn.input+=inputlevel.get(i).output*wMatrix1.matrix[i][j];
				//System.out.print(wMatrix1.matrix[i][j]+" ");
			}
		//	System.out.println();
		}
		//System.out.println();
		for(int i=0;i<midlevel.size();i++){
			midNode mn=midlevel.get(i);
			mn.input+=mn.b;
			mn.a=(1.0/(1.0+Math.exp(-mn.input)));
			mn.output=mn.a;
		//	System.out.print(mn.a+" ");
		}
	//	System.out.println();
		for(int i=0;i<midlevel.size();i++){
			for(int j=0;j<outlevel.size();j++){
				outNode on=outlevel.get(j);
				on.input+=midlevel.get(i).output*wMatrix2.matrix[i][j];
		//		System.out.print(wMatrix2.matrix[i][j]+" ");
			}
		//	System.out.println();
		}
	//	System.out.println();
	//	System.out.println();
	//	System.out.println();
		//System.out.println();
		for(int i=0;i<outlevel.size();i++){
			outNode on=outlevel.get(i);
			on.input+=on.b;
		//	System.out.println(on.b+" b");
		//	System.out.println(on.input+" i");
			on.a=(1.0/(1.0+Math.exp(-on.input)));
			on.output=on.a;
			System.out.print(on.a+" ");
		}
		System.out.println();
		for( int i=0;i<outlevel.size();i++){
			outNode on=outlevel.get(i);
			on.u=on.a*(1-on.a)*(vector.code.get(i)-on.a);
		//	System.out.print(vector.code.get(i)+"                 ");
			on.b+=l*on.u;	
		}
		//System.out.println();
		for(int i=0;i<midlevel.size();i++){
			for(int j=0;j<outlevel.size();j++){
				midNode mn=midlevel.get(i);
				mn.u+=wMatrix2.matrix[i][j]*outlevel.get(j).u;
				wMatrix2.matrix[i][j]+=l*outlevel.get(j).u*midlevel.get(i).output;	
			}
		}
		for(int i=0;i<midlevel.size();i++){
			midNode mn=midlevel.get(i);
			mn.u*=l*mn.output*(1-mn.output);
			mn.b+=l*mn.u;
		}
		for(int i=0;i<inputlevel.size();i++){
			for(int j=0;j<midlevel.size();j++){
				wMatrix1.matrix[i][j]+=l*midlevel.get(j).u*inputlevel.get(i).output;
			}
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public void test(ArrayList<Vector>vectors){
		right=0;worry=0;
		for(Vector vector:vectors){
			
			for(int i=0;i<midlevel.size();i++){
				midNode temp=midlevel.get(i);
				temp.input=0;
				temp.u=0;
			//	midlevel.put(i,temp);
			}
			for(int i=0;i<outlevel.size();i++){
				outNode temp=outlevel.get(i);
				temp.input=0;
				temp.u=0;
			//	outlevel.put(i,temp);
			}
			
		for(int i=0;i<inputlevel.size();i++){
			inputNode in=inputlevel.get(i);
			in.output=vector.attr.get(i);
		}
		for(int i=0;i<inputlevel.size();i++){
			for(int j=0;j<midlevel.size();j++){
				midNode mn=midlevel.get(j);
				mn.input+=inputlevel.get(i).output*wMatrix1.matrix[i][j];
			}
		}
		for(int i=0;i<midlevel.size();i++){
			midNode mn=midlevel.get(i);
			mn.input+=mn.b;
			mn.a=(1/(1+Math.exp(-mn.input)));
			mn.output=mn.a;
		}
		for(int i=0;i<midlevel.size();i++){
			for(int j=0;j<outlevel.size();j++){
				outNode on=outlevel.get(j);
				on.input+=midlevel.get(i).output*wMatrix2.matrix[i][j];
			}
		}
		double max=Integer.MIN_VALUE;
		int index1 = 0,index2 = 0;
		for(int i=0;i<outlevel.size();i++){
			outNode on=outlevel.get(i);
			on.input+=on.b;
			on.a=(1/(1+Math.exp(-on.input)));
			on.output=on.a;
			if(vector.code.get(i).equals(1.0))
				index2=i;
			if(max<on.a){
				index1=i;
				max=on.a;
			}
			System.out.print(on.a+" "+vector.code.get(i)+" ");
		}
		System.out.println(index1+" "+index2);
		if(index1==index2)
			right++;
		else{
			worry++;
		}
		System.out.println();
		}
	}
	
}
