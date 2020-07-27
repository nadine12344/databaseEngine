package T_221B;

import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.Vector;

public class BPTree<T extends Comparable<T>> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int order;
	private BPTreeNode<T> root;

	/**
	 * Creates an empty B+ tree
	 * @param order the maximum number of keys in the nodes of the tree
	 */
	public BPTree(int order) 
	{
		this.order = order;
		root = new BPTreeLeafNode<T>(this.order);
		root.setRoot(true);
	}
	
	/**
	 * Inserts the specified key associated with the given record in the B+ tree
	 * @param key the key to be inserted
	 * @param recordReference the reference of the record associated with the key
	 * @throws IOException 
	 */
	public void insert(String name,T key, Ref recordReference) throws IOException
	{
		PushUp<T> pushUp = root.insert(name,key, recordReference, null, -1);
		if(pushUp != null)
		{
			BPTreeInnerNode<T> newRoot = new BPTreeInnerNode<T>(order);
			newRoot.insertLeftAt(0, pushUp.key, root);
			newRoot.setChild(1, pushUp.newNode);
			root.setRoot(false);
			root = newRoot;
			root.setRoot(true);
		}
	}
	
	
	/**
	 * Looks up for the record that is associated with the specified key
	 * @param key the key to find its record
	 * @return the reference of the record associated with this key 
	 */
	public String search(T key)
	{
		return root.search(key);
	}
	//for>=
	public Vector<String> searchMoreOrEqual(T key)
	{
		return root.searchMoreOrEqual(key);
	}
	//for=
	public Vector<String> searchMore(T key)
	{
		return root.searchMore(key);
	}
	public Ref getIndexForInsert(T key,int size) {
		return root.getIndexForInsert(key,-1,size);
	}
	
	/**
	 * Delete a key and its associated record from the tree.
	 * @param key the key to be deleted
	 * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
	 * @throws IOException 
	 */
	public boolean delete(String name,T key,Ref r) throws IOException
	{
		boolean done = root.delete(name,key,r, null, -1);
		//go down and find the new root in case the old root is deleted
		while(root instanceof BPTreeInnerNode && !root.isRoot())
			root = ((BPTreeInnerNode<T>) root).getFirstChild();
		return done;
	}
	//for<
	public Vector<String> searchLess(T key)
	{BPTreeNode<T> curNode=this.root;
			while(curNode instanceof BPTreeInnerNode)
			{
				BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
				curNode=parent.getChild(0);	
			}
			
				BPTreeLeafNode<T> child = (BPTreeLeafNode<T>) curNode;
				return child.searchLess(key);	
	}
	//for <=
	public Vector<String> searchLessOrEqual(T key)
	{BPTreeNode<T> curNode=this.root;
			while(curNode instanceof BPTreeInnerNode)
			{
				BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
				curNode=parent.getChild(0);	
			}
			
				BPTreeLeafNode<T> child = (BPTreeLeafNode<T>) curNode;
				return child.searchLessOrEqual(key);	
	}
	public void  shiftForInsert(String string, int startPage,int index,int lastPage,int maxSize) throws IOException{
		hashtablePage h=null;
		h=(hashtablePage) DBApp.deserialize("data/"+string+"tree"+"Pages"+".class", h);
		for(int l=lastPage;l>=startPage;l--) {

			if(!(h.h.containsKey(l))&& l==lastPage) {
				h.h.put(l, new Hashtable<>());}
			else {
				Set<String>s=h.h.get(l).keySet();
				Vector<String>v=new Vector<>();
				for(String key: s){
					v.add(key);	
				}
				for(int c=0;c<v.size();c++) {
					Ref2 r2=null;
				r2=(Ref2) DBApp.deserialize(v.get(c), r2);
				if(l==startPage) {
				for(int i=0;i<r2.ref.size();i++) {
					if(r2.ref.get(i).getPage()==l) {
					if(r2.ref.get(i).getIndexInPage()>=index&&r2.ref.get(i).getIndexInPage()<(maxSize-1)) {
						r2.ref.get(i).setIndexInPage(r2.ref.get(i).getIndexInPage()+1);	
//						System.out.println(r2.ref.get(i).getIndexInPage()+"lolo");
					}
					else if(!(l==lastPage)&&r2.ref.get(i).getIndexInPage()==maxSize-1) {	
						r2.ref.get(i).setPage(r2.ref.get(i).getPage()+1);
						r2.ref.get(i).setIndexInPage(0);
						//System.out.println(r2.ref.get(i).getPage());
						h.increment(l+1, v.get(c));
						h.decrement(l,v.get(c) );
						//System.out.println(l+1);
						}
					
	}}
}
				else {
				//System.out.println("in");
					for(int i=0;i<r2.ref.size();i++) {
						if(r2.ref.get(i).getPage()==l) {
						if(r2.ref.get(i).getIndexInPage()<(maxSize-1)) {
							//System.out.println(r2.ref.get(i).getIndexInPage());
							r2.ref.get(i).setIndexInPage(r2.ref.get(i).getIndexInPage()+1);
							//System.out.println("------"+r2.ref.get(i).getIndexInPage());
						}
						else if(r2.ref.get(i).getIndexInPage()==maxSize-1) {
							//System.out.println(r2.ref.get(i).getIndexInPage());
							r2.ref.get(i).setPage(r2.ref.get(i).getPage()+1);
							r2.ref.get(i).setIndexInPage(0);
							//System.out.println("------"+r2.ref.get(i).getIndexInPage());
							h.decrement(l,v.get(c) );
							h.increment(l+1, v.get(c));
							}
		}}
	}
				//System.out.println(v.get(c));
				DBApp.serialize(r2, v.get(c));}

}
DBApp.serialize(h, "data/"+string+"tree"+"Pages"+".class");
				}
				
			}

	public void shiftInPageForDelete(String string, int i, int l) throws IOException {
		hashtablePage h=null;
		h=(hashtablePage) DBApp.deserialize("data/"+string+"tree"+"Pages"+".class", h);
		Set<String>s=h.h.get(i).keySet();
		int count=0;
		//may use vector if an error appeared
		for(String key: s){
			Ref2 r2=null;
			r2=(Ref2) DBApp.deserialize(key, r2);
			for(int k=0;k<r2.ref.size();k++) {
				
				if(r2.ref.get(k).getPage()== i && r2.ref.get(k).getIndexInPage()>l){
					count++;
				}
				r2.ref.get(k).setIndexInPage(r2.ref.get(k).getIndexInPage()-count);
				count=0;
			}
			
			DBApp.serialize(r2, key);
		}
		DBApp.serialize(h, "data/"+string+"tree"+"Pages"+".class");
	}

	public void shiftPagesForDelete(String string, int m) throws IOException {
		hashtablePage h=null;
		h=(hashtablePage) DBApp.deserialize("data/"+string+"tree"+"Pages"+".class", h);
		Set<Integer>s=h.h.keySet();
		int count=0;
		//may use vector if an error appeared
		Vector<Integer>v2=new Vector<>();
		for(Integer key: s){
			
			if(v2.size()==0) {
				v2.add(key);
				
			}
			else
			for(int j=0;j<v2.size();j++) {
				if(j==v2.size()) {
					v2.add(j+1,key);
					break;
				}
				else
		if(v2.get(j)>key) {
			System.out.println(key);
			v2.add(j,key);
			break;
		}
		}}
		for(int c=0;c<v2.size();c++) {
			
	if(v2.get(c)>m) {
				count++;
			
			if(count>0) {
				Set<String>s2=h.h.get(v2.get(c)).keySet();
				Vector<String>v=new Vector<>();
				for(String key1: s2){
					v.add(key1);
				}
				for(int l=0;l<v.size();l++) {
					Ref2 r2=null;
					r2=(Ref2) DBApp.deserialize(v.get(l), r2);
					for(int k=0;k<r2.ref.size();k++) {
						if(r2.ref.get(k).getPage()==v2.get(c)) {
							h.decrement(v2.get(c), v.get(l));
							h.increment(r2.ref.get(k).getPage()-count,v.get(l));
							r2.ref.get(k).setPage(r2.ref.get(k).getPage()-count);
						}
					}
					
					DBApp.serialize(r2, v.get(l));
				}
				count=0;
			}
			
		}}
		DBApp.serialize(h, "data/"+string+"tree"+"Pages"+".class");
	}

		
		
	
//	public void  shiftForInsert(String string, int startPage,int index,int lastPage,int maxSize) throws IOException
//	{System.out.println("start"+startPage);
//	System.out.println("end"+lastPage);
//	System.out.println("index"+index);
//		hashtablePage h=null;
//		h=(hashtablePage) DBApp.deserialize("data/"+string+"tree"+"Pages"+".class", h);
//		for(int l=lastPage;l>=startPage;l--) {
//			System.out.println("l="+l);
//				if(!(h.h.containsKey(l))&& l==lastPage) {
//					h.h.put(l, new Hashtable<>());
//					
//				}
//				else{
//				Set<String>s=h.h.get(l).keySet();
//				for(String key: s){
//					Ref2 r2=null;
//							r2=(Ref2) DBApp.deserialize(key, r2);
//						
//							if(l==startPage) {
//								System.out.println("[[[[[[[[");
//							for(int i=0;i<r2.ref.size();i++) {
//								System.out.println("innnnnn");
//								if(r2.ref.get(i).getIndexInPage()>=index&&r2.ref.get(i).getIndexInPage()<(maxSize-1)) {
//									System.out.println(r2.ref.get(i).getIndexInPage()+"lolo");
//									r2.ref.get(i).setIndexInPage(r2.ref.get(i).getIndexInPage()+1);	
//									System.out.println(r2.ref.get(i).getIndexInPage()+"lolo");
//								}
//								else if(!(l==lastPage)&&r2.ref.get(i).getIndexInPage()==maxSize-1) {	
//									
//									r2.ref.get(i).setPage(r2.ref.get(i).getPage()+1);
//									r2.ref.get(i).setIndexInPage(0);
//									System.out.println(r2.ref.get(i).getPage());
//									h.increment(l+1, key);
//									h.decrement(l,key );
//									System.out.println(l+1);}
//								
//				}
//		}
//							else {
//								System.out.println("in");
//								for(int i=0;i<r2.ref.size();i++) {
//									if(r2.ref.get(i).getIndexInPage()<(maxSize-1)) {
//										System.out.println(r2.ref.get(i).getIndexInPage());
//										r2.ref.get(i).setIndexInPage(r2.ref.get(i).getIndexInPage()+1);
//										System.out.println("------"+r2.ref.get(i).getIndexInPage());
//									}
//									else if(r2.ref.get(i).getIndexInPage()==maxSize-1) {
//										System.out.println(r2.ref.get(i).getIndexInPage());
//										r2.ref.get(i).setPage(r2.ref.get(i).getPage()+1);
//										r2.ref.get(i).setIndexInPage(0);
//										System.out.println("------"+r2.ref.get(i).getIndexInPage());
//										h.decrement(l,key );
//										h.increment(l+1, key);}
//					}
//				}
//							System.out.println(key);
//							DBApp.serialize(r2, key);}
//			
//		}
//		DBApp.serialize(h, "data/"+string+"cre"+"Pages"+".class");
//		}}
	/**
	 * Returns a string representation of the B+ tree.
	 */
	public String toString()
	{	
		
		//	<For Testing>
		// node :  (id)[k1|k2|k3|k4]{P1,P2,P3,}
		String s = "";
		Queue<BPTreeNode<T>> cur = new LinkedList<BPTreeNode<T>>(), next;
		cur.add(root);
		while(!cur.isEmpty())
		{
			next = new LinkedList<BPTreeNode<T>>();
			while(!cur.isEmpty())
			{
				BPTreeNode<T> curNode = cur.remove();
				System.out.print(curNode);
				if(curNode instanceof BPTreeLeafNode)
					System.out.print("->");
				else
				{
					System.out.print("{");
					BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
					for(int i = 0; i <= parent.numberOfKeys; ++i)
					{
						System.out.print(parent.getChild(i).index+",");
						next.add(parent.getChild(i));
					}
					System.out.print("} ");
				}
				
			}
			System.out.println();
			cur = next;
		}	
		//	</For Testing>
		return s;
	}

}
