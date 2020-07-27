package T_221B;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class row implements Serializable{
	Hashtable<String, Object> hashtable;
	pagePointer  pointer;
	public row(Hashtable<String, Object> hashtable,pagePointer pointer) {
		this.hashtable=hashtable;
		this.pointer=pointer;
		
	}
	}

