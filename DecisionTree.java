package decisionTree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


import com.google.common.base.Preconditions;

public class DecisionTree {

	public abstract class Tree{
		public Tree(){};
	}
	public class categoricalTree extends Tree{
		public String name;
		public Map<String,Tree> children=new HashMap<String,Tree>();
	}
	public class numericalTree extends Tree{
		public String name;
		public Tree leftChild;
		public Tree rightChild;
		public Double IG;
	}
	public class leaf extends Tree{
		public String result;
		public leaf( String result){
			this.result=result;
		}
	}
	public class Data{
		public ArrayList<String> type=new ArrayList<String>();
		public ArrayList<Vector> set=new ArrayList<Vector>();
	}
	public Tree buildTree(Data data,Map<String,Integer> attrs){
		if(data.set.size()<5){
			Map<String,Integer> labels=new HashMap<String,Integer>();
			for(Vector vector:data.set){
				if(!labels.containsKey(vector.vector.get("Label"))){
					labels.put(vector.vector.get("Lables"), 1);
				}
				else{
					labels.put(vector.vector.get("label"),labels.get(vector.vector.get("label"))+1);
				}
			}
			int max=0;
			String label = null;
			for(Map.Entry<String,Integer> entry:labels.entrySet()){
				if(entry.getValue()>max){
					
					label=entry.getKey();
					max=entry.getValue();
				}
			}
		   return new leaf(label);
		}
		
		Double max=Double.MIN_VALUE;
		String name = null;
		for(Map.Entry<String,Integer> entry:attrs.entrySet()){
			Double ig = null;
			if(data.type.get(entry.getValue())=="Numerical"){
				ig=computeNumerical(data,entry.getKey());
			}
			else if(data.type.get(entry.getValue())=="Categorical"){
				ig=computeCategorical(data,entry.getKey());
			}
			if(ig>max){
				name=entry.getKey();
			}
		}
		attrs.remove(name);
		
		if(data.type.get(attrs.get(name))=="Numerical"){
			numericalTree node=new numericalTree();
			node.IG=max;
			node.name=name;
			Data datal=new Data();
			Data datar=new Data();
			datal.type=data.type;
			datar.type=data.type;
			//node.leftChild=new Tree();
			//node.rightChild=new Tree();
			for(Vector vector:data.set){
				if(Double.parseDouble(vector.vector.get(name))>node.IG){
					datar.set.add(vector);
				}
				else{
					datal.set.add(vector);
				}
			}
			node.leftChild=buildTree(datal,attrs);
			node.rightChild=buildTree(datar,attrs);
			return node;
		}
		else if(data.type.get(attrs.get(name))=="Categorical"){
		
			Map<String,Tree> children=new HashMap<String,Tree>();
			Map<String,Data> datas=new HashMap<String,Data>();
			for(Vector vector:data.set){
				if(!datas.containsKey(vector.vector.get(name))){
					Data temp=new Data();
					temp.set.add(vector);
					datas.put(name, temp);
				}else{
					datas.get(name).set.add(vector);
				}
			}
			for(Map.Entry<String, Data> entry:datas.entrySet()){
				entry.getValue().type=data.type;
				children.put(entry.getKey(),buildTree(entry.getValue(),attrs));
			}
			categoricalTree node=new categoricalTree();
			node.name=name;
			node.children=children;
			return node;
		}
		return null;
	}
	public Double computeCategorical(Data data,String name) {
		// TODO Auto-generated method stub
		int countall[];
		int count[][];
		ArrayList<String> labels=new ArrayList<String>();
		ArrayList<String> attrs=new ArrayList<String>();
		for(Vector vector:data.set){
			if(!attrs.contains(vector.vector.get(name))){
				attrs.add(vector.vector.get(name));
			}
			if(!labels.contains(vector.vector.get("labels"))){
			    labels.add(vector.vector.get("labels"));
			}
		}
		countall=new int[labels.size()];
		count=new int[attrs.size()][labels.size()];
		for(Vector vector:data.set){
			countall[labels.indexOf(vector.vector.get("labels"))]++;
			count[attrs.indexOf(vector.vector.get(name))][labels.indexOf(vector.vector.get("labels"))]++;
		}
		 int size = data.set.size();
		    double hy = entropy(countall, size); // H(Y)
		    double hyx = 0.0; // H(Y|X)
		    double invDataSize = 1.0 / size;

		    for (int index = 0; index < attrs.size(); index++) {
		      size = sum(count[index]);
		      hyx += size * invDataSize * entropy(count[index], size);
		    }

		    double ig = hy - hyx;
		
		return ig;
	}
	public Double computeNumerical(Data data,String name) {
		// TODO Auto-generated method stub
		 int countall[];
		 int count[][];
		 int countless[];
		 ArrayList<String> labels=new ArrayList<String>();
			ArrayList<String> attrs=new ArrayList<String>();
			for(Vector vector:data.set){
				if(!attrs.contains(vector.vector.get(name))){
					attrs.add(vector.vector.get(name));
				}
				if(!labels.contains(vector.vector.get("labels"))){
				    labels.add(vector.vector.get("labels"));
				}
			}
			countall=new int[labels.size()];
			countless=new int[labels.size()];
			count=new int[attrs.size()][labels.size()];
			for(Vector vector:data.set){
				countall[labels.indexOf(vector.vector.get("labels"))]++;
				count[attrs.indexOf(vector.vector.get(name))][labels.indexOf(vector.vector.get("labels"))]++;
			}
		
			 int size = data.set.size();
			    double hy = entropy(countall, size);
			    double invDataSize = 1.0 / size;

			    int best = -1;
			    double bestIg = -1.0;

			    // try each possible split value
			    for (int index = 0; index < attrs.size(); index++) {
			      double ig = hy;

			      // instance with attribute value < values[index]
			      size = sum(countless);
			      ig -= size * invDataSize * entropy(countless, size);

			      // instance with attribute value >= values[index]
			      size = sum(countall);
			      ig -= size * invDataSize * entropy(countall, size);

			      if (ig > bestIg) {
			        bestIg = ig;
			        best = index;
			      }

			      add(countless, count[index]);
			      dec(countall, count[index]);
			    }

			   
		return bestIg;
	}
	public int sum(int[] count){
		int sum=0;
		for(int i=0;i<count.length;i++)
			sum+=count[i];
		return sum;
	}
	private static double entropy(int[] counts, int dataSize) {
	    if (dataSize == 0) {
	      return 0.0;
	    }

	    double entropy = 0.0;
	    double invDataSize = 1.0 / dataSize;

	    for (int count : counts) {
	      if (count == 0) {
	        continue; // otherwise we get a NaN
	      }
	      double p = count * invDataSize;
	      entropy += -p * Math.log(p) / Math.log(2.0);
	    }

	    return entropy;
	  }
	 public static void add(int[] array1, int[] array2) {
		    Preconditions.checkArgument(array1.length == array2.length, "array1.length != array2.length");
		    for (int index = 0; index < array1.length; index++) {
		      array1[index] += array2[index];
		    }
		  }
		  
		  /**
		   * foreach i : array1[i] -= array2[i]
		   */
		  public static void dec(int[] array1, int[] array2) {
		    Preconditions.checkArgument(array1.length == array2.length, "array1.length != array2.length");
		    for (int index = 0; index < array1.length; index++) {
		      array1[index] -= array2[index];
		    }
		  }
		  

	
}
