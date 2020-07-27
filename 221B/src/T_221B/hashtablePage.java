package T_221B;

import java.io.Serializable;
import java.util.Hashtable;

public class hashtablePage implements Serializable {
	Hashtable<Integer, Hashtable<String,Integer>>h=new Hashtable<>();
	public void increment(int i,String s) {
		if(h.containsKey(i)) {
		if(h.get(i).containsKey(s)) {

		h.get(i).put(s, h.get(i).get(s)+1);	}
		else {
		
			h.get(i).put(s,1);}}
		else {
			Hashtable <String,Integer>h2=new Hashtable<>();
			h2.put(s, 1);
			h.put(i, h2);}
	}
	public void decrement(int i,String s) {
		h.get(i).put(s, h.get(i).get(s)-1);	
		if(h.get(i).get(s).equals(0))
			h.get(i).remove(s);
		if(h.get(i).size()==0)
			h.remove(i);
	}
}
