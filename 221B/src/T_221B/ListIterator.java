package T_221B;
import java.util.Hashtable;
import java.util.Iterator;

public class ListIterator<T> implements Iterator<T> { 
	NodeSelect<T> current; 
      
    // initialize pointer to head of the list for iteration 
    public ListIterator(List<T> list) 
    { 
        current = list.getHead(); 
    } 
      
    // returns false if next element does not exist 
    public boolean hasNext() 
    { 
        return current != null; 
    } 
      
    // return current data and update pointer 
    public T next() 
    { 
        T data = current.getData(); 
        current = current.getNext(); 
        return data; 
    } 
      
    // implement if needed 
    public void remove() 
    { 
        throw new UnsupportedOperationException(); 
    } 
  
        public static void main(String[] args) 
        { 
            // Create Linked List 
        	
            List<Hashtable<String, String>> myList = new List<>(); 
              Hashtable<String,String> h=new Hashtable<>();
              Hashtable<String, String> htblColNameType = new Hashtable<String, String>( ); 
      		htblColNameType.put("id", "java.lang.Integer"); 
      		htblColNameType.put("name", "java.lang.String"); 
      		htblColNameType.put("gpa", "java.lang.Double"); 
            myList.add(htblColNameType); 
            myList.add(htblColNameType); 
            myList.add(htblColNameType);
            ListIterator<Hashtable<String, String>> l=new ListIterator<>( myList);
         while(!(l.current==null)) {
        	 System.out.println(l.current.data);
        	 l.current=l.current.next;
         }
              
            // Iterate through the list using For Each Loop 
            for (Hashtable<String, String> string : myList) 
                System.out.println(string); 
        } 
    } 

