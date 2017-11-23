package commom;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

public class Vector {
	
    int size;
	
	ArrayList element;
	
	public Vector(){
		this.size=0;
		 element=new ArrayList();
	}
	public void setVectorSize(int size){
		element=new ArrayList(size);
	}
	public Vector copy(){
		Vector v1=new Vector();
		//v1.size=size;
		//System.out.println("size"+this.element.size());
		for(int i=0;i<size;i++)
			v1.add(this.getQuick(i));
		return v1;
	}
	

	public int size() {
		// TODO Auto-generated method stub
		return size;
	}

	public void add( double value) {
		// TODO Auto-generated method stub
		element.add(value);
		size++;
		
	}
	public void setQuick(int index,double value){
		element.set(index, value);
	}

	public double getQuick(int index) {
		// TODO Auto-generated method stub
		return (Double) element.get(index);
	}
	
}
