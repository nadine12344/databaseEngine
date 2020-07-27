package T_221B;
import java.util.Iterator;

public class List<T> implements Iterable<T> { 
	NodeSelect<T> head, tail; 
      
    // add new Element at tail of the linked list in O(1) 
    public void add(T data) 
    { 
    	NodeSelect<T> node = new NodeSelect<>(data, null); 
        if (head == null) 
            tail = head = node; 
        else { 
            tail.setNext(node); 
            tail = node; 
        } 
    } 
    // return Head 
    public NodeSelect<T> getHead() 
    { 
        return head; 
    } 
      
    // return Tail 
    public NodeSelect<T> getTail() 
    { 
        return tail; 
    } 
      
    // return Iterator instance 
    public Iterator<T> iterator() 
    { 
        return new ListIterator<T>(this); 
    } 
} 
