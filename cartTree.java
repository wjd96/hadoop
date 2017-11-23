package decisionTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import decisionTree.DecisionTree.Tree;

public class cartTree {

	
	public static class Tree{
		public String name;//attrname
		public Integer SUM,MIN;
		public Double a;
		public Tree LeftChild;
		public Tree RightChild;
		public Tree(){}
		public Tree(String name,Integer SUM,Integer MIN){
			this.name=name;
			this.SUM=SUM;
			this.MIN=MIN;
		}
	}
	public class CategoricalTree extends Tree{
		public CategoricalTree(String name,Integer SUM,Integer MIN) {
			super(name, SUM,MIN);
			// TODO Auto-generated constructor stub
		}
		public String attr;
	}
	public class NumericalTree extends Tree{
		public NumericalTree(String name,Integer SUM,Integer MIN) {
			super(name, SUM,MIN);
			// TODO Auto-generated constructor stub
		}
		public Double attr;
	}
	public class Leaf extends Tree{
		public Leaf(String name,Integer SUM,Integer MIN){
			super(name, SUM,MIN);
		}
	}
	public static  class Data{
		public Map<String,String> type=new HashMap<String,String>();
		public ArrayList<Vector> set=new ArrayList<Vector>();
		public Data(){}
	}
	public Tree build(Data data){
		//pair p=getMxMn(data);
		int min=Integer.MAX_VALUE,max=0;
		String maxlabel = null;
		Map<String,Integer> labels=getLabels(data);
		for(Map.Entry<String, Integer> entry:labels.entrySet()){
			if(entry.getValue()<min)
				min=entry.getValue();
			if(entry.getValue()>max){
				max=entry.getValue();
				maxlabel=entry.getKey();
			}
		//	sum+=entry.getValue();
		}
		if(data.set.size()<4){	
			Leaf leaf=new Leaf(maxlabel,data.set.size(),min) ;
			leaf.a=(double)leaf.MIN/(double)leaf.SUM;
			return leaf;
		}
		split minsplit=compute(data);////////////////////////////////////////////
		Data datal=new Data();
		Data datar=new Data();
		
		
		if(data.type.get(minsplit.name).equals("Categorical")){
			
			
			
			Catsplit sp=(Catsplit) minsplit;
			data.type.remove(minsplit.name);
			for(Vector vector:data.set){
				if(vector.vector.get(sp.name).equals(sp.point))
					datal.set.add(vector);
				else
					datar.set.add(vector);
			}
			datal.type=data.type;
			datar.type=data.type;
		//	int mintype=datal.set.size()>datar.set.size()?datal.set.size():datar.set.size();
			CategoricalTree cateT=new CategoricalTree(sp.name,data.set.size(),min);
			cateT.attr=sp.point;
			cateT.LeftChild=build(datal);
			cateT.RightChild=build(datar);
			return cateT;
		}else{
			Numsplit sp=(Numsplit) minsplit;
			for(Vector vector:data.set){
				if(Double.parseDouble(vector.vector.get(sp.name))<sp.point)
					datal.set.add(vector);
				else
					datar.set.add(vector);
			}
			datal.type=data.type;
			datar.type=data.type;
			//int mintype=datal.set.size()>datar.set.size()?datal.set.size():datar.set.size();
			NumericalTree NumT=new NumericalTree(sp.name,data.set.size(),min);
			NumT.attr=sp.point;
			NumT.LeftChild=build(datal);
			NumT.RightChild=build(datar);
			return NumT;
		}
		//return null;
		
	}
/*	public pair getMxMn(Data data){
		Map<String,Integer> labels=getLabels(data);
		Integer Max,Min;
		for()
	}*/
	public abstract class split{
		public double gain;
		public String name;
		public split(double gain,String name){
			this.gain=gain;
			this.name=name;
		}
	}
	public class Numsplit extends split{
		public double point;
	//	public double gain;
		public Numsplit(double point,double gain,String name){
			super(gain,name);
			this.point=point;
		//	this.gain=gain;
		}
	}
	public class Catsplit extends split{
		public String point;
	//	public double gain;
		public Catsplit(String point,double gain,String name){
			super(gain,name);
			this.point=point;
			//this.gain=gain;
		}
	}
//	public 
	private split compute(Data data) {
		// TODO Auto-generated method stub
		split minsplit=null;
		split split=null;
		double mingain=Double.MAX_VALUE;
		for(Map.Entry<String,String> entry:data.type.entrySet()){
			if(entry.getKey().equals("labels"))
				continue;
			if(entry.getValue().equals("Categorical")){
				split=computeCategorical(entry.getKey(),data);
			}else if(entry.getValue().equals("Numerical")){
				split=computeNumerical(entry.getKey(),data);
			}
			if(mingain>split.gain){
				minsplit=split;
				mingain=split.gain;
			}
		}	
		return minsplit;
	}
	private Numsplit computeNumerical(String str, Data data) {
		// TODO Auto-generated method stub
		//int count
		ArrayList<Double> attrs=getNAttrs(data,str);
		ArrayList<String> labels=getCAttrs(data,"labels");
		double min=Double.MAX_VALUE;
		Double point=null;
		int count[][];
		int countall[];
		double attributes[] = new double[attrs.size()] ;
		int i=0;
		for(Double d:attrs){
			attributes[i++]=d;
		}
		for(int j=0;j<attributes.length-1;j++){
			double mid=(attributes[j]+attributes[j+1])/2;
			countall=new int[2];
			count=new int [2][labels.size()];
			for(Vector vector:data.set){
				if(Double.parseDouble(vector.vector.get(str))<=mid)	{
					count[0][labels.indexOf(vector.vector.get("labels"))]++;//attr
					countall[0]++;
				}
				else{
					count[1][labels.indexOf(vector.vector.get("labels"))]++;//other
					countall[1]++;
				}				
			}
			Double temp=gain(count,countall);
			if(temp<min){
				point=mid;
				min=temp;
			}
		}
		return new Numsplit(point,min,str);//?
	}
	/////////////////////////////////*·ÖÀà*///////////////////////////////////////////////////////////////////
	private Catsplit computeCategorical(String str, Data data) {
		// TODO Auto-generated method stub
		ArrayList<String> attrs=getCAttrs(data,str);
		ArrayList<String> labels=getCAttrs(data,"labels");
		double min=Double.MAX_VALUE;
		String name=null;
		int count[][];
		int countall[];
		for(String str1:attrs){
		//	if(str1.equals("labels"))
		//		continue;
			countall=new int[2];
			count=new int [2][labels.size()];
			for(Vector vector:data.set){
				if(vector.vector.get(str).equals(str1))	{
					count[0][labels.indexOf(vector.vector.get("labels"))]++;//attr
					countall[0]++;
				}
				else{
					count[1][labels.indexOf(vector.vector.get("labels"))]++;//other
					countall[1]++;
				}				
			}
			Double temp=gain(count,countall);
			if(temp<min){
				name=str1;
				min=temp;
			}
		}
		return new Catsplit(name,min,str);//?
	}
	private Double gain(int[][] count,int [] countall) {
		// TODO Auto-generated method stub
		double sum=countall[0]+countall[1];
		double gain=0;
		double result=0;
		for(int i=0;i<count[0].length;i++){
			result+=Math.pow((double)count[0][i]/(double)countall[0],2);
		}
		gain+=countall[0]/sum*(1-result);
		result=0;
		for(int i=0;i<count[1].length;i++){
			result+=Math.pow((double)count[1][i]/(double)countall[0],2);
		}
		gain+=countall[1]/sum*(1-result);
		return gain;
	}
	public Map<String,Integer> getLabels(Data data){
		Map<String,Integer> labels=new HashMap<String,Integer>();
		for(Vector vector:data.set){
			if(!labels.containsKey(vector.vector.get("labels"))){
				labels.put(vector.vector.get("labels"), 1);
			}else{
				labels.put(vector.vector.get("labels"),labels.get(vector.vector.get("labels"))+1);
			}
		}
        List<Map.Entry<String,Integer>> list =new ArrayList<Map.Entry<String,Integer>>(labels.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
        public int compare(Map.Entry<String, Integer> o1,
            Map.Entry<String, Integer> o2) {
              return (o2.getValue() - o1.getValue());
            }
        });
		return labels;
	}
	public ArrayList<Double> getNAttrs(Data data,String name){
		//ArrayList<Object> attrs=new ArrayList<Object>();
			ArrayList<Double> attrs=new ArrayList<Double>();
			for(Vector vector:data.set){
				if(!attrs.contains(Double.parseDouble(vector.vector.get(name)))){
					attrs.add(Double.parseDouble(vector.vector.get(name)));
				}
			}
			Collections.sort(attrs,new NComparator());
			return attrs;
	}
	public ArrayList<String> getCAttrs(Data data,String name){
		//ArrayList<Object> attrs=new ArrayList<Object>();
			ArrayList<String> attrs=new ArrayList<String>();
			for(Vector vector:data.set){
				if(!attrs.contains(vector.vector.get(name))){
					attrs.add(vector.vector.get(name));
				}
			}
			Collections.sort(attrs,new CComparator());
			return attrs;
	}
	class NComparator implements Comparator{ 
		public int compare(Object o1,Object o2) { 
		Double e1=(Double)o1; 
		Double e2=(Double)o2; 
		if(e1>e2) 
		return 1; 
		else 
		return 0; 
		} 	
	} 
	class CComparator implements Comparator{ 
		public int compare(Object o1,Object o2) { 
		String e1=(String)o1; 
		String e2=(String)o2; 
		if(e1.compareTo(e2)>0) 
		return 1; 
		else 
		return 0; 
		} 	
	} 
	/****************************************cart*****************************************/
	
	public class pair{
		double leafnum;
		double sumMIN;
		public pair(){}
		public pair(int leafnum,int sumMIN){
			this.leafnum=leafnum;
			this.sumMIN=sumMIN;
		}
	}
    public pair computea(Tree tree,double total){
    	if(tree.getClass().toString().equals(Leaf.class.toString())){
    	//	System.out.println(tree.getClass().toString());
    		return new pair(1,tree.MIN);
    	}
    	pair pl=computea(tree.LeftChild,total);
    	pair pr=computea(tree.RightChild,total);
    	pair temp=new pair();
    	temp.leafnum=pl.leafnum+pr.leafnum;
    	temp.sumMIN=pl.sumMIN+pr.sumMIN;
    	tree.a=((tree.MIN-temp.sumMIN)/(total*(temp.leafnum-1)));
    	if(tree.a<mina&&(tree.getClass().toString()!=Leaf.class.toString())){
    		//System.out.println("1111111111111");
    		mina=tree.a;
    		branch=tree;
    	}
    	return temp;  	
    }
    public static Tree branch;
    public static Double mina=Double.MAX_VALUE;
    public static Map<String,Integer> branchlabel=new HashMap<String,Integer>();
    public void cart(Tree head,double total){
    	computea(head,total);
    	//if(branch==null)
    		//System.out.print(branch.getClass().toString());
    	cartbranch(branch);
    	int max=0;
    	String label = null;
    	for(Map.Entry<String,Integer> entry:branchlabel.entrySet()){
    		if(entry.getValue()>max){
    			max=entry.getValue();
    			label=entry.getKey();
    		}
    	}
    	branch.name=label;
    	branch.LeftChild=null;
        branch.RightChild=null;
    }
	private void cartbranch(Tree node) {
		if(node.getClass().toString().equals((Leaf.class.toString()))){
			if(!branchlabel.containsKey(node.name)){
				branchlabel.put(node.name, 1);
			}else{
				branchlabel.put(node.name, branchlabel.get(node.name)+1);
			}
		}
		else{
			cartbranch(node.LeftChild);
			cartbranch(node.RightChild);
		}
	}	
	public void searchTree(Tree node){
		if(node==null)
			return;
		if(node.getClass().toString().equals(Leaf.class.toString())){
			System.out.println(node.name);
			return ;
		}
		if(node.getClass().toString().equals(CategoricalTree.class.toString())){
			CategoricalTree ct=(CategoricalTree) node;
			System.out.println(ct.name+" "+ct.attr);
		}
        if(node.getClass().toString().equals(NumericalTree.class.toString())){
			NumericalTree nt=(NumericalTree) node;
			System.out.println(nt.name+" "+nt.attr);
		}
		searchTree(node.LeftChild);
		searchTree(node.RightChild);		
	}
	//////////*»Ø¹é*//////////////////////////
	public split CateCompute(Data data,String str){
		ArrayList<String> attrs=getCAttrs(data,str);
	//	ArrayList<String> labels=getCAttrs(data,"labels");
		double min=Double.MAX_VALUE;
		String name=null;
	//	int count[][];
	//	int countall[];
		double suml=0,sumr=0,sum=Double.MIN_VALUE;
		for(String str1:attrs){
			suml=0;sumr=0;
			for(Vector vector:data.set){
				if(vector.vector.get(str).equals(str1)){
					suml+=Double.parseDouble(vector.vector.get("labels"));
				}else{
					sumr+=Double.parseDouble(vector.vector.get("labels"));
				}
			}
			if(sum<suml+sumr){
				name=str1;
				sum=suml+sumr;
			}
		}
		return new Catsplit(name,Math.sqrt(sum),str);//?
	}
	public split NumCompute(Data data,String str){
		ArrayList<Double> attrs=getNAttrs(data,str);
		double min=Double.MAX_VALUE;
		double attributes[] = new double[attrs.size()] ;
		int i=0;
		for(Double d:attrs){
			attributes[i++]=d;
		}
		double suml=0,sumr=0,sum=Double.MIN_VALUE,point = 0;
		for(int j=0;j<attributes.length-1;j++){
			double mid=(attributes[j]+attributes[j+1])/2;
			for(Vector vector:data.set){
				if(Double.parseDouble(vector.vector.get(str))<=mid)	{
					suml+=Double.parseDouble(vector.vector.get("labels"));
				}
				else{
					sumr+=Double.parseDouble(vector.vector.get("labels"));
				}				
			}
			if(sum<suml+sumr){
				point=mid;
				sum=suml+sumr;
			}
		}
		return new Numsplit(point,Math.sqrt(sum),str);//?
	}
}
