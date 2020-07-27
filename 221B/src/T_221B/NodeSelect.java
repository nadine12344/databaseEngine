package T_221B;

public class NodeSelect<T> { 
    T data; 
    NodeSelect<T> next; 
    public NodeSelect(T data, NodeSelect<T> next) 
    { 
        this.data = data; 
        this.next = next; 
    } 
      
    // Setter getter methods for Data and Next Pointer 
    public void setData(T data) 
    { 
        this.data = data; 
    } 
      
    public void setNext(NodeSelect<T> next) 
    { 
        this.next = next; 
    } 
      
    public T getData() 
    { 
        return data; 
    } 
      
    public NodeSelect<T> getNext() 
    { 
        return next; 
    } 
} 
