package decisionTree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import decisionTree.cartTree.Data;
import decisionTree.cartTree.Tree;
import decisionTree.cartTree.*;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Data data=new Data();
		Map<String,String> type=new HashMap<String,String>();
		type.put("home","Categorical");
		type.put("mar", "Categorical");
		type.put("income","Numerical");
		type.put("labels", "Categorical");
		ArrayList<Vector> vectors=new ArrayList<Vector>();
		Vector vector=new Vector();
		vector.vector.put("home", "yes");
		vector.vector.put("mar", "single");
		vector.vector.put("income", "125");
		vector.vector.put("labels","yes");
		Vector vector1=new Vector();
		vector1.vector.put("home", "no");
		vector1.vector.put("mar", "mad");
		vector1.vector.put("income", "100");
		vector1.vector.put("labels","no");
		Vector vector2=new Vector();
		vector2.vector.put("home", "no");
		vector2.vector.put("mar", "single");
		vector2.vector.put("income", "70");
		vector2.vector.put("labels","no");
		Vector vector3=new Vector();
		vector3.vector.put("home", "yes");
		vector3.vector.put("mar", "mad");
		vector3.vector.put("income", "120");
		vector3.vector.put("labels","no");
		Vector vector4=new Vector();
		vector4.vector.put("home", "no");
		vector4.vector.put("mar", "div");
		vector4.vector.put("income", "95");
		vector4.vector.put("labels","yes");
		Vector vector5=new Vector();
		vector5.vector.put("home", "no");
		vector5.vector.put("mar", "mad");
		vector5.vector.put("income", "60");
		vector5.vector.put("labels","no");
		Vector vector6=new Vector();
		vector6.vector.put("home", "yes");
		vector6.vector.put("mar", "div");
		vector6.vector.put("income", "220");
		vector6.vector.put("labels","no");
		Vector vector7=new Vector();
		vector7.vector.put("home", "no");
		vector7.vector.put("mar", "single");
		vector7.vector.put("income", "85");
		vector7.vector.put("labels","yes");
		Vector vector8=new Vector();
		vector8.vector.put("home", "no");
		vector8.vector.put("mar", "mad");
		vector8.vector.put("income", "75");
		vector8.vector.put("labels","no");
		Vector vector9=new Vector();
		vector9.vector.put("home", "no");
		vector9.vector.put("mar", "single");
		vector9.vector.put("income", "90");
		vector9.vector.put("labels","yes");;
		vectors.add(vector9);
		vectors.add(vector8);
		vectors.add(vector7);
		vectors.add(vector6);
		vectors.add(vector5);
		vectors.add(vector4);
		vectors.add(vector3);
		vectors.add(vector2);
		vectors.add(vector1);
		vectors.add(vector);
	    data.set=vectors;
	    data.type=type;
	    cartTree.Tree head=new cartTree.Tree();
	    cartTree ct=new cartTree();
	    head=ct.build(data);
	    ct.cart(head, 10);
	    ct.searchTree(head);
	    System.out.println("ddddd");
		
		
	}

}
