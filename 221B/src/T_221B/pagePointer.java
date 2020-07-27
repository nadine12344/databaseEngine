package T_221B;
import java.io.Serializable;
import java.util.Hashtable;

public class pagePointer implements Serializable {
	
	int index;
	String page;
	public pagePointer(int index,String page) {
		this.index=index;
		this.page=page;
	}
	
}
