package T_221B;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Vector;



public class BPTreeLeafNode<T extends Comparable<T>> extends BPTreeNode<T> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String[] records;
	private BPTreeLeafNode<T> next;
	
	@SuppressWarnings("unchecked")
	public BPTreeLeafNode(int n) 
	{
		super(n);
		keys = new Comparable[n];
		records = new String[n];

	}
	
	/**
	 * @return the next leaf node
	 */
	public BPTreeLeafNode<T> getNext()
	{
		return this.next;
	}
	
	/**
	 * sets the next leaf node
	 * @param node the next leaf node
	 */
	public void setNext(BPTreeLeafNode<T> node)
	{
		this.next = node;
	}
	
	/**
	 * @param index the index to find its record
	 * @return the reference of the queried index
	 */
	public String getRecord(int index) 
	{
		return records[index];
	}
	
	
	/**
	 * sets the record at the given index with the passed reference
	 * @param index the index to set the value at
	 * @param recordReference the reference to the record
	 */
	public void setRecord(int index, String recordReference) 
	{
		records[index] = recordReference;
	}

	/**
	 * @return the reference of the last record
	 */
	public String getFirstRecord()
	{
		return records[0];
	}

	/**
	 * @return the reference of the last record
	 */
	public String getLastRecord()
	{
		return records[numberOfKeys-1];
	}
	
	/**
	 * finds the minimum number of keys the current node must hold
	 */
	public int minKeys()
	{
		if(this.isRoot())
			return 1;
		return (order + 1) / 2;
	}
	
	/**
	 * insert the specified key associated with a given record refernce in the B+ tree
	 * @throws IOException 
	 */
	public PushUp<T> insert(String name,T key, Ref recordReference, BPTreeInnerNode<T> parent, int ptr) throws IOException
	{ hashtablePage h=null;
	h=(hashtablePage) DBApp.deserialize("data/"+name+"tree"+"Pages"+".class", h);
	
	
		int index = 0;
			while (index < numberOfKeys && getKey(index).compareTo(key) < 0)
				++index;
			if(index < numberOfKeys && getKey(index).compareTo(key)== 0) {
				h.increment(recordReference.getPage(), "data/"+name+"ref2"+getKey(index)+".class");
				DBApp.serialize(h, "data/"+name+"tree"+"Pages"+".class");
				this.insertAt2(name,index, key, recordReference);
			}
			else {
				h.increment(recordReference.getPage(), "data/"+name+"ref2"+key+".class");
				DBApp.serialize(h, "data/"+name+"tree"+"Pages"+".class");
				String ref2="data/"+name+"ref2"+key+".class";
				Vector<Ref> ref=new Vector<>();
				ref.add(recordReference);
				Ref2 r2=new Ref2(ref);
				DBApp.serialize(r2, ref2);
				if(this.isFull())
				{
					BPTreeNode<T> newNode = this.split(name,key, ref2);
					Comparable<T> newKey = newNode.getFirstKey();
					return new PushUp<T>(newNode, newKey);
				}
			this.insertAt(index, key, ref2);}
			return null;
		
	}
	
	public Ref getIndexForInsert(T key, int ptr,int maxSize) {
		Ref2 ref2=null;
		int index = 0;
		while (index < numberOfKeys && getKey(index).compareTo(key) < 0)
			++index;
		if(index < numberOfKeys && getKey(index).compareTo(key)== 0) {
		ref2=(Ref2) DBApp.deserialize(records[index], ref2);
			if(ref2.ref.get(ref2.ref.size()-1).getIndexInPage()==(maxSize-1)) {
			
			return new Ref(ref2.ref.get(ref2.ref.size()-1).getPage()+1,0);	
			}
			else {
				return new Ref(ref2.ref.get(ref2.ref.size()-1).getPage(),ref2.ref.get(ref2.ref.size()-1).getIndexInPage()+1);}	
		}else {
			if(index==0) {
				if(records[index]==null)
					return new Ref(0,0);
				ref2=(Ref2) DBApp.deserialize(records[index], ref2);
						return new Ref(ref2.ref.get(0).getPage(),ref2.ref.get(0).getIndexInPage());	
				}
			else {
				ref2=(Ref2) DBApp.deserialize(records[index-1], ref2);
				if(ref2.ref.get(ref2.ref.size()-1).getIndexInPage()==(maxSize-1)) {
					return new Ref(ref2.ref.get(ref2.ref.size()-1).getPage()+1,0);	
					}
					else
						return new Ref(ref2.ref.get(ref2.ref.size()-1).getPage(),ref2.ref.get(ref2.ref.size()-1).getIndexInPage()+1);	
				}
			}
	}

	private void insertAt2(String name, int index, T key, Ref recordReference) throws IOException {
		Ref2 reference2=null;
		reference2=(Ref2) DBApp.deserialize(records[index], reference2);

		for(int i=0;i<=reference2.ref.size();i++) {
		
			if(i==reference2.ref.size()) {
				reference2.ref.add(reference2.ref.size(), recordReference);
				break;}
		else if(reference2.ref.get(i).getPage()==recordReference.getPage() && reference2.ref.get(i).getIndexInPage()>recordReference.getIndexInPage()) {
				reference2.ref.add(i, recordReference);
				break;}
			else if (reference2.ref.get(i).getPage()>recordReference.getPage()) {
				reference2.ref.add(i, recordReference);
				break;
		}}

		DBApp.serialize(reference2, records[index]);
	}

	/**
	 * inserts the passed key associated with its record reference in the specified index
	 * @param name 
	 * @param index the index at which the key will be inserted
	 * @param key the key to be inserted
	 * @param string the pointer to the record associated with the key
	 * @throws IOException 
	 */
	private void insertAt( int index, Comparable<T> key, String string) throws IOException 
	{
		
		for (int i = numberOfKeys - 1; i >= index; --i) 
		{
			this.setKey(i + 1, getKey(i));
			this.setRecord(i + 1, getRecord(i));
		}

		this.setKey(index, key);
		this.setRecord(index, string);
		++numberOfKeys;
	}
	
	/**
	 * splits the current node
	 * @param key the new key that caused the split
	 * @param r2 the reference of the new key
	 * @return the new node that results from the split
	 * @throws IOException 
	 */
	public BPTreeNode<T> split(String name,T key, String r2) throws IOException 
	{
		int keyIndex = this.findIndex(key);
		int midIndex = numberOfKeys / 2;
		if((numberOfKeys & 1) == 1 && keyIndex > midIndex)	//split nodes evenly
			++midIndex;		

		
		int totalKeys = numberOfKeys + 1;
		//move keys to a new node
		BPTreeLeafNode<T> newNode = new BPTreeLeafNode<T>(order);
		for (int i = midIndex; i < totalKeys - 1; ++i) 
		{
			newNode.insertAt(i - midIndex, this.getKey(i), this.getRecord(i));
			numberOfKeys--;
		}
		
		//insert the new key
		if(keyIndex < totalKeys / 2)
			this.insertAt(keyIndex, key, r2);
		else
			newNode.insertAt(keyIndex - midIndex, key, r2);
		
		//set next pointers
		newNode.setNext(this.getNext());
		this.setNext(newNode);
		
		return newNode;
	}
	
	/**
	 * finds the index at which the passed key must be located 
	 * @param key the key to be checked for its location
	 * @return the expected index of the key
	 */
	public int findIndex(T key) 
	{
		for (int i = 0; i < numberOfKeys; ++i) 
		{
			int cmp = getKey(i).compareTo(key);
			if (cmp > 0) 
				return i;
		}
		return numberOfKeys;
	}

	/**
	 * returns the record reference with the passed key and null if does not exist
	 */
	@Override
	public String search(T key) 
	{
		for(int i = 0; i < numberOfKeys; ++i)
			if(this.getKey(i).compareTo(key) == 0)
				return this.getRecord(i);
		return null;
	}
	public Vector<String> searchMoreOrEqual(T key) {
		Vector<String> s=new Vector<>();
		BPTreeLeafNode b=(BPTreeLeafNode) this;
		int i = 0;
		for(i = 0; i < numberOfKeys; ++i)
			if(this.getKey(i).compareTo(key) >= 0) {
				s.add( this.getRecord(i));
				break;}
		for(i = i+1; i < numberOfKeys; ++i)
			s.add( this.getRecord(i));
		while(b.next!=null) {
			b=b.next;
			for(int h=0;h<b.numberOfKeys;h++)
				s.add(b.records[h]);
		}
		return s;
	}
	public Vector<String> searchMore(T key) {
		Vector<String> s=new Vector<>();
		BPTreeLeafNode b=(BPTreeLeafNode) this;
		int i = 0;
		for(i = 0; i < numberOfKeys; ++i)
			if(this.getKey(i).compareTo(key) > 0) {
				s.add( this.getRecord(i));
				break;}
		for(i = i+1; i < numberOfKeys; ++i)
			s.add( this.getRecord(i));
		while(b.next!=null) {
			b=b.next;
			for(int h=0;h<b.numberOfKeys;h++)
				s.add(b.records[h]);
		}
		return s;
	}
	public Vector<String> searchLess(T key) {
		Vector<String> s=new Vector<>();
		BPTreeLeafNode b=(BPTreeLeafNode) this;
		for(int i = 0; i < numberOfKeys; ++i) {
			if(this.getKey(i).compareTo(key) <0) {
				s.add( this.getRecord(i));}
			else return s;}
		
		while(b.next!=null) {
			b=b.next;
				for(int i = 0; i < b.numberOfKeys; ++i) {
					if(b.getKey(i).compareTo(key) <0) {
						s.add( b.getRecord(i));}
					else return s;}
		}
		return s;
	}
	public Vector<String> searchLessOrEqual(T key) {
		Vector<String> s=new Vector<>();
		BPTreeLeafNode b=(BPTreeLeafNode) this;
		for(int i = 0; i < numberOfKeys; ++i) {
			if(this.getKey(i).compareTo(key) <=0) {
				s.add( this.getRecord(i));}
			else return s;}
		
		while(b.next!=null) {
			b=b.next;
				for(int i = 0; i < b.numberOfKeys; ++i) {
					if(b.getKey(i).compareTo(key) <=0) {
						s.add( b.getRecord(i));}
					else return s;}
		}
		return s;
	}
	
	/**
	 * delete the passed key from the B+ tree
	 * @throws IOException 
	 */
	public boolean delete(String name,T key,Ref r, BPTreeInnerNode<T> parent, int ptr) throws IOException 
	{hashtablePage h=null;
	h=(hashtablePage) DBApp.deserialize("data/"+name+"tree"+"Pages"+".class", h);
		for(int i = 0; i < numberOfKeys; ++i)
			if(keys[i].compareTo(key) == 0)
			{ Ref2 r2=null;
			    r2=(Ref2) DBApp.deserialize(records[i], r2);
				
			    for(int i2=0;i2<r2.ref.size();i2++) {
			    	if(r2.ref.get(i2).getIndexInPage()==r.getIndexInPage()&&r2.ref.get(i2).getPage()==r.getPage()) {
			    		h.decrement(r.getPage(), records[i]);
			    		DBApp.serialize(h, "data/"+name+"tree"+"Pages"+".class");
			    		r2.ref.remove(i2);
			    		DBApp.serialize(r2, records[i]);
			    		
			    		break;
			    	}
			    }
			    if(r2.ref.size()==0) {
			    	File f=new File(records[i]);
			    	f.delete();
				this.deleteAt(i);
				if(i == 0 && ptr > 0)
				{
					//update key at parent
					parent.setKey(ptr - 1, this.getFirstKey());
				}
				//check that node has enough keys
				if(!this.isRoot() && numberOfKeys < this.minKeys())
				{
					//1.try to borrow
					if(borrow(parent, ptr))
						return true;
					//2.merge
					merge(parent, ptr);
				}
				}
			 
			
				return true;
			}
		return false;
	}
	
	/**
	 * delete a key at the specified index of the node
	 * @param index the index of the key to be deleted
	 */
	public void deleteAt(int index)
	{
		for(int i = index; i < numberOfKeys - 1; ++i)
		{
			keys[i] = keys[i+1];
			records[i] = records[i+1];
		}
		numberOfKeys--;
	}
	
	/**
	 * tries to borrow a key from the left or right sibling
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 * @return true if borrow is done successfully and false otherwise
	 * @throws IOException 
	 */
	public boolean borrow(BPTreeInnerNode<T> parent, int ptr) throws IOException
	{
		//check left sibling
		if(ptr > 0)
		{
			BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr-1);
			if(leftSibling.numberOfKeys > leftSibling.minKeys())
			{
				this.insertAt(0, leftSibling.getLastKey(), leftSibling.getLastRecord());		
				leftSibling.deleteAt(leftSibling.numberOfKeys - 1);
				parent.setKey(ptr - 1, keys[0]);
				return true;
			}
		}
		
		//check right sibling
		if(ptr < parent.numberOfKeys)
		{
			BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr+1);
			if(rightSibling.numberOfKeys > rightSibling.minKeys())
			{
				this.insertAt(numberOfKeys, rightSibling.getFirstKey(), rightSibling.getFirstRecord());
				rightSibling.deleteAt(0);
				parent.setKey(ptr, rightSibling.getFirstKey());
				return true;
			}
		}
		return false;
	}
	
	/**
	 * merges the current node with its left or right sibling
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 * @throws IOException 
	 */
	public void merge(BPTreeInnerNode<T> parent, int ptr) throws IOException
	{
		if(ptr > 0)
		{
			//merge with left
			BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr-1);
			leftSibling.merge(this);
			parent.deleteAt(ptr-1);			
		}
		else
		{
			//merge with right
			BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr+1);
			this.merge(rightSibling);
			parent.deleteAt(ptr);
		}
	}
	
	/**
	 * merge the current node with the specified node. The foreign node will be deleted
	 * @param foreignNode the node to be merged with the current node
	 * @throws IOException 
	 */
	public void merge(BPTreeLeafNode<T> foreignNode) throws IOException
	{
		for(int i = 0; i < foreignNode.numberOfKeys; ++i)
			this.insertAt(numberOfKeys, foreignNode.getKey(i), foreignNode.getRecord(i));
		
		this.setNext(foreignNode.getNext());
	}

	

	

	


	
}
