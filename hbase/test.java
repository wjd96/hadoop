package hbaseNew;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class test {
	public static class team{
		String name;
		int score=0;
		int won=0;
		int goal=0;
	public team(){
		score=0;
		won=0;
		goal=0;
	}
		
	}
	public static int mup(int i){
		int sum=0;
		for(int j=i-1;j>=1;j--){
			sum+=j;
		}
		return sum;
		
	}
	public static team getTeam(String name,ArrayList<team> teams){
		for(int i=0;i<teams.size();i++){
			if(name.equals(teams.get(i).name))
				return teams.get(i);
		}
		return null;
	}

	public static void sort(ArrayList<team> teams){
		Collections.sort(teams,new Comparator<team>(){  
            public int compare(team arg0, team arg1) {  
            	  if(arg0.score==arg1.score){
            		  if(arg0.won==arg1.won){
            			  if(arg0.goal==arg1.goal){
            				 return arg0.name.compareTo(arg1.name);
            			  }else{
            				  if(arg0.goal>arg1.goal)
            					  return 1;
            				  else
            					  return 0;
            			  }
            			
            		  }else{
            			  if(arg0.won>arg1.won)
                			  return 1;
                		  else
                			  return 0;
            		  }
            	  }else{
            		  if(arg0.score>arg1.score)
            			  return 1;
            		  else
            			  return 0;
            	  }
			//return 0;
            }

        });  
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
/*
		int totalnum = 4;
		ArrayList<team> teams=new ArrayList<team>();
		for(int i=0;i<totalnum;i++){
			team t=new team();
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String str = null;
			System.out.println("Enter your value:");
			t.name = br.readLine();
			teams.add(t);
		//	System.out.println(t.name);
		}
		int totalm=mup(totalnum);
		String temp;
		System.out.println(totalm);
		System.out.println("please enter score");
		for(int i=0;i<totalm;i++){
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String str = null;
		//	System.out.println("Enter your value:");a
			temp = br.readLine();
			int a=Integer.parseInt(temp.substring(4, 5));
			int b=Integer.parseInt(temp.substring(6));
			String namea=temp.substring(0,1);
		//	System.out.println(namea+" "+a);
			
			String nameb=temp.substring(2,3);
		//	System.out.println(nameb+" "+b);
			team teama=getTeam(namea,teams);
			team teamb=getTeam(nameb,teams);
			teama.goal+=a;
			teamb.goal+=b;
			if(a==b){
				teama.score+=1;
				teamb.score+=1;
			}else if(a<b){
				teamb.won+=1;
				teamb.score+=3;
			}else{
				teama.won+=1;
				teama.score+=3;
			}
		}
		Collections.sort(teams,new Comparator<team>(){ 
			@Override
            public int compare(team arg0, team arg1) {  
            	  if(arg0.score==arg1.score){
              if(arg0.won==arg1.won){
            			  if(arg0.goal==arg1.goal){
            				 return arg0.name.compareTo(arg1.name);
            			  }else{
            				  if(arg0.goal<arg1.goal)
            					  return 1;
            				  else
            					  return -1;
            			  }
            			
            		  }else{
            			  if(arg0.won<arg1.won)
                			  return 1;
                		  else
                			  return -1;
            		  }
            	  }else{
            		  if(arg0.score<arg1.score)
            			  return 1;
            		  else
            			  return -1;
            	  }
			//return 0;
			//	return totalm;
            }

        });  
		for(int i=0;i<teams.size();i++){
			System.out.println(teams.get(i).name+" "+teams.get(i).score+" "+teams.get(i).won+" "+teams.get(i).goal);
		}*/
//		int i=0; Integer j=new Integer(0);System.out.println(i==j);System.out.println(j.equals(i));

	}
	

}
