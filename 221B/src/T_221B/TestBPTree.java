package T_221B;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

public class TestBPTree {

	public static void main(String[] args) throws IOException 
	{
		hashtablePage h=new hashtablePage();
		DBApp.serialize(h,"data/"+"vvvvbPlusPages"+".class");
		h=(hashtablePage) DBApp.deserialize("data/"+"vvvvbPlusPages"+".class",h);
		BPTree<Integer> tree = new BPTree<>(4);
		DBApp.serialize(tree, "data/"+"vvvvs"+".class");
	tree=(BPTree<Integer>) DBApp.deserialize("data/"+"vvvvs"+".class",tree);
		Scanner sc = new Scanner(System.in);
		while(true) 
		{
			int x = sc.nextInt();
			if(x == -1)
				break;
			int y=sc.nextInt();
			int z=sc.nextInt();
			System.out.println(y+"     "+z);
			Ref r=new Ref(y, z);
			tree.insert("vvvv",x, r);
			System.out.println(tree.toString());
		}
		DBApp.serialize(tree, "data/"+"vvvvs"+".class");
		tree=(BPTree<Integer>) DBApp.deserialize("data/"+"vvvvs"+".class",tree);
		hashtablePage h2=null;
		h2=(hashtablePage) DBApp.deserialize("data/"+"vvvvbPlusPages"+".class",h2);
		Vector<String> v2=(tree.searchMore(-44));
		for(int i=0;i<v2.size();i++) {
			Ref2 r2=null;
			r2=(Ref2) DBApp.deserialize(v2.get(i), r2);
			System.out.println(v2.get(i));
			for(int j=0;j<r2.ref.size();j++) {
				System.out.println(r2.ref.get(j).getPage());
				System.out.println(r2.ref.get(j).getIndexInPage());
			}
			
		}
		  Set<Integer> keys7 = h2.h.keySet();
		  for(Integer key: keys7){
			  System.out.println("page"+key);
			  Set<String> keys3 = h2.h.get(key).keySet();
	
			  for(String key4: keys3){
				  System.out.println("ref"+key4);
				  System.out.println( "count"+h2.h.get(key).get(key4));
				  
			  }
		  }
				

		Vector<Integer>v5= new Vector<>();
		v5.add(0);
		v5.add(1);
	tree.shiftInPageForDelete("vvvv", 0, 1);
	DBApp.serialize(tree, "data/"+"vvvvs"+".class");
	tree=(BPTree<Integer>) DBApp.deserialize("data/"+"vvvvs"+".class",tree);
	
	h=(hashtablePage) DBApp.deserialize("data/"+"vvvvbPlusPages"+".class",h);
		Vector<String> v=(tree.searchMore(-44));
		for(int i=0;i<v.size();i++) {
			Ref2 r2=null;
			r2=(Ref2) DBApp.deserialize(v.get(i), r2);
			System.out.println(v.get(i));
			for(int j=0;j<r2.ref.size();j++) {
				System.out.println(r2.ref.get(j).getPage());
				System.out.println(r2.ref.get(j).getIndexInPage());
			}
			
		}
		  Set<Integer> keys1 = h.h.keySet();
		  for(Integer key: keys1){
			  System.out.println("page"+key);
			  Set<String> keys2 = h.h.get(key).keySet();
	
			  for(String key2: keys2){
				  System.out.println("ref"+key2);
				  System.out.println( "count"+h.h.get(key).get(key2));
				  
			  }
		  }
				

		  while(true) 
			{
				int x = sc.nextInt();
				if(x == -1)
					break;
				int y=sc.nextInt();
				int z=sc.nextInt();
				Ref r=new Ref(y, z);
				tree.delete("vvvv",x,r);
				System.out.println(tree.toString());
			}
			sc.close();
			DBApp.serialize(tree, "data/"+"vvvvs"+".class");
			h=(hashtablePage) DBApp.deserialize("data/"+"vvvvbPlusPages"+".class",h);
				Vector<String> v4=(tree.searchMore(-44));
				for(int i=0;i<v4.size();i++) {
					Ref2 r2=null;
					r2=(Ref2) DBApp.deserialize(v4.get(i), r2);
					System.out.println(v4.get(i));
					for(int j=0;j<r2.ref.size();j++) {
						System.out.println(r2.ref.get(j).getPage());
						System.out.println(r2.ref.get(j).getIndexInPage());
					}
					
				}
				  Set<Integer> keys3 = h.h.keySet();
				  for(Integer key: keys3){
					  System.out.println("page"+key);
					  Set<String> keys2 = h.h.get(key).keySet();
			
					  for(String key2: keys2){
						  System.out.println("ref"+key2);
						  System.out.println( "count"+h.h.get(key).get(key2));
						  
					  }
				  }
		}		
	}
	
