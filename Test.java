package NN;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import NN.NN.Vector;

public class Test {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

/*		Configuration conf=new Configuration();
		   FileSystem fs=FileSystem.get(conf); 
		     
     ArrayList<Vector> vectors=new ArrayList<Vector>();
				    Path path=new Path("input2/12.txt");  
				    FSDataInputStream fsd =((FileSystem) fs).open(path);  
				    BufferedReader bis = new BufferedReader(new InputStreamReader(fsd,"UTF-8")); 
				    String temp;
				    while ((temp = bis.readLine()) != null) {  
				    	  String[] strs=temp.split(",");
				    	  Vector vector=new Vector();
				    	  for(int i=0;i<strs.length-1;i++){
				    		  vector.attr.add(Double.parseDouble(strs[i]));
				    	  }
				    	  if(strs[strs.length-1].equals("Iris-versicolor")){
				    		  vector.code.add((double) 1);
				    		  vector.code.add((double) 0);
				    		  vector.code.add((double) 0);
				    		  
				    	  } else if(strs[strs.length-1].equals("Iris-virginica")){
				    		  vector.code.add((double) 0);
				    		  vector.code.add((double) 0);
				    		  vector.code.add((double) 1);
				    		  
				    	  } else if(strs[strs.length-1].equals("Iris-setosa")){
				    		  vector.code.add((double) 0);
				    		  vector.code.add((double) 1);
				    		  vector.code.add((double) 0);
				    		  
				    	  }
				
				    	vectors.add(vector);  
				    }
		/*		    for(Vector v:vectors){
				    	for(Double d:v.attr){
				    		System.out.print(d+" ");
				    	}
				    	for(Double d:v.code){
				    		System.out.print(d+" ");
				    	}
				    	System.out.println();
	/*			    }*/
	/*			    NN nn=new NN();
				    nn.init(4, 5, 3);
				    for(int i=0;i<1020;i++){
				   
				    	nn.forword(vectors);
				   // 	System.out.println();
				   // 	System.out.println();
				   // 	System.out.println();
				    }
				    System.out.println("over");
				   
				    
				    
				    
				    
				    ArrayList<Vector> vectors1=new ArrayList<Vector>();
				    Path path1=new Path("input2/13.txt");  
				    FSDataInputStream fsd1 =((FileSystem) fs).open(path1);  
				    BufferedReader bis1 = new BufferedReader(new InputStreamReader(fsd1,"UTF-8")); 
				    String temp1;
				    while ((temp1= bis1.readLine()) != null) {  
				    	  String[] strs=temp1.split(",");
				    	  Vector vector=new Vector();
				    	  for(int i=0;i<strs.length-1;i++){
				    		  vector.attr.add(Double.parseDouble(strs[i]));
				    	  }
				    	  if(strs[strs.length-1].equals("Iris-versicolor")){
				    		  vector.code.add((double) 1);
				    		  vector.code.add((double) 0);
				    		  vector.code.add((double) 0);
				    		  
				    	  } else if(strs[strs.length-1].equals("Iris-virginica")){
				    		  vector.code.add((double) 0);
				    		  vector.code.add((double) 0);
				    		  vector.code.add((double) 1);
				    		  
				    	  } else if(strs[strs.length-1].equals("Iris-setosa")){
				    		  vector.code.add((double) 0);
				    		  vector.code.add((double) 1);
				    		  vector.code.add((double) 0);
				    		  
				    	  }
				
				    	vectors1.add(vector);  
				    }
			/*	    for(Vector v:vectors1){
				    	for(Double d:v.attr){
				    		System.out.print(d+" ");
				    	}
				    	for(Double d:v.code){
				    		System.out.print(d+" ");
				    	}
				    	System.out.println();
				    }*/
				    System.out.println();
/*		nn.test(vectors1);
		System.out.println(nn.right+" "+nn.worry);
		System.out.println((double)nn.right/((double)nn.right+(double)nn.worry));
		
		*/
		
		
		
		Configuration conf=new Configuration();
		   FileSystem fs=FileSystem.get(conf); 

           ArrayList<Vector> vectors=new ArrayList<Vector>();
           ArrayList<ArrayList<String>> attrs=new ArrayList<ArrayList<String>>();
           Map<Integer,ArrayList<String>> type=new HashMap<Integer,ArrayList<String>>();
				    Path path=new Path("input2/11.txt");  
				    FSDataInputStream fsd =((FileSystem) fs).open(path);  
				    BufferedReader bis = new BufferedReader(new InputStreamReader(fsd,"UTF-8")); 
				    String temp;
				    while ((temp = bis.readLine()) != null) {  
				    	String[] strs=temp.split(" ");
				    	ArrayList<String> attr=new ArrayList<String>();
				    	for(int i=1;i<strs.length;i++){
				    		if(!type.containsKey(i)){
				    			ArrayList<String> t=new ArrayList<String>();
				    			t.add(strs[i]);
				    			type.put(i, t);
				    		}else{
				    			ArrayList<String> t=type.get(i);
				    			if(!t.contains(strs[i])){
				    				t.add(strs[i]);
				    			}
				    			type.put(i, t);
				    		}
				    		attr.add(strs[i]);
				    	}
				    	attrs.add(attr);
				    }
				    for(ArrayList<String> st:attrs){
				    	Vector v=new Vector();
				    //	System.out.println();
				    	for(int i=0;i<st.size()-1;i++){
				    //		System.out.print(st.get(i));
				    		ArrayList<String> t=type.get(i+1);
				    		for(String s:t){
				    			if(s.equals(st.get(i))){
				    				v.attr.add(1.0);
				    			}else{
				    				v.attr.add(0.0);
				    			}
				    		}
				    	}
				    	ArrayList<String> t=type.get(type.size());
				    	for(String s:t){
				    		System.out.println(s+"S");
				    		if(s.equals(st.get(st.size()-1)))
				    			v.code.add(1.0);
				    		else
				    			v.code.add(0.0);
				    	}
				    	vectors.add(v);
				    }
				    for(Vector v:vectors){
				    	for(Double d:v.attr)
				    		System.out.print(d+" ");
				    	for(Double d:v.code)
				    		System.out.print(d+" ");
				    	System.out.println();
				    }
				    NN nn=new NN();
				    nn.init(9, 3, 2);
				    for(int i=0;i<2000;i++){
						   
				    	nn.forword(vectors);
				   	System.out.println();
				    	System.out.println();
				   // 	System.out.println();
				    }
				    System.out.println("over");
				   
					   System.out.println(fs.getUri().toString());    
	}
	

}
