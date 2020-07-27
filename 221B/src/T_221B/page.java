package T_221B;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import javax.sql.RowSet;

public class page implements Serializable{
	int maxSize;
Vector<Hashtable<String, Object>> rows=new Vector<>();//// size=200

public page(int size) throws IOException {
maxSize=size;
}
public boolean lessThan(Hashtable<String,Object> htblColNameValue,String key ) {
	Comparable upperrange=(Comparable) rows.get(rows.size()-1).get(key);
	Comparable<Comparable> x = (Comparable<Comparable>)htblColNameValue.get(key);
	if(x.compareTo(upperrange)<0||(x.compareTo(upperrange)==0 && rows.size()<maxSize)) {
		return true;
	}
	else {
		return false;	
	}
	}
public boolean lessThan2(Hashtable<String,Object> htblColNameValue,String key ) {
	Comparable upperrange=(Comparable) rows.get(rows.size()-1).get(key);
	Comparable<Comparable> x = (Comparable<Comparable>)htblColNameValue.get(key);
	if(x.compareTo(upperrange)<0) {
		return true;
	}
	else {
		return false;	
	}
	}
public boolean checkHashtable(Set<String> keys,Hashtable<String,Object> h1,Hashtable<String,Object> h2) {
	for(String key: keys){
		if(!(((Comparable) h1.get(key)).compareTo((Comparable)h2.get(key))==0)) {
			
			return false;
		}
		
		if(h1.get(key).getClass().toString().equals("class T_221B.polygon"))
			if(!((polygon)(h1).get(key)).toString().equals(((polygon)h2.get(key)).toString()))
				return false;
	}
	return true;
}
public boolean lessThan3(Comparable x,String key) {
	Comparable upperrange=(Comparable) rows.get(rows.size()-1).get(key);
	if(x.compareTo(upperrange)<0) {
		return true;
	}
	else {
		return false;	
	}
}
public boolean[] searchInPage(Vector<Hashtable<String, Object>> search, Comparable x, String columnName) {
	boolean[] b= {false,false,false};
	boolean keyFound=false;
	int middleIndex2;
	int leftIndex = 0;
    int rightIndex = rows.size() - 1;
    Comparable middle=null;
    int middleIndex = 0;
    while(leftIndex <= rightIndex)
    { middleIndex = ((leftIndex + rightIndex)) / 2;       
      middle=(Comparable) (rows.get(middleIndex)).get(columnName); 
        if (x.compareTo(middle)<0)
            rightIndex = middleIndex-1;
        else if(x.compareTo(middle)>0)
            leftIndex = middleIndex +1;
        else {
        
        	keyFound=true;
        	if(middleIndex==0)
        		b[1]=true;
        	
        	b[0]=true;
            	rightIndex--;
            	search.add(rows.get(middleIndex));
	break;
 	      	
        } 	
    }
  
//System.out.println(middleIndex);
    middleIndex2=(middleIndex+1);
   if(keyFound==true && rows.size()>middleIndex2) {
   
    middle=(Comparable) (rows.get(middleIndex2)).get(columnName); 
   while(x.compareTo(middle)==0 && rows.size()>=middleIndex2+1) {
	   search.add(rows.get(middleIndex2)); 
	   middleIndex2++; 
			 middle=(Comparable) (rows.get(middleIndex2)).get(columnName); 

   }}

   if( rows.size()>0)
   for(int i=middleIndex-1;i>=0;i--) {
	   if(x.compareTo((Comparable) (rows.get(i)).get(columnName))==0) {
		   if(i==0) {
			   b[1]=true;
		   }
		 search.add(rows.get(i)); 
	   }
	   else
		   break;
	   
   }
   b[2]=keyFound;
   return b;
}
public boolean[] searchInPage2(Vector<Hashtable<String, Object>> search, Comparable x, String columnName) {
	boolean[] b= {false,false,false};
	boolean keyFound=false;
	int middleIndex2;
	int leftIndex = 0;
    int rightIndex = rows.size() - 1;
    Comparable middle=null;
    int middleIndex = 0;
    while(leftIndex <= rightIndex)
    { middleIndex = ((leftIndex + rightIndex)) / 2;       
      middle=(Comparable) (rows.get(middleIndex)).get(columnName); 
        if (x.compareTo(middle)<0)
            rightIndex = middleIndex-1;
        else if(x.compareTo(middle)>0)
            leftIndex = middleIndex +1;
        else {
        
        	keyFound=true;
        	if(middleIndex==0)
        		b[1]=true;
        	
        	b[0]=true;
            	rightIndex--;
            	search.add(rows.get(middleIndex));
	break;
 	      	
        } 	
    }
  
//System.out.println(middleIndex);
    middleIndex2=(middleIndex+1);
   if(keyFound==true && rows.size()>middleIndex2) {
   
    middle=(Comparable) (rows.get(middleIndex2)).get(columnName); 
   while(x.compareTo(middle)==0 && rows.size()>=middleIndex2+1) {
	   search.add(rows.get(middleIndex2)); 
	   middleIndex2++; 
			 middle=(Comparable) (rows.get(middleIndex2)).get(columnName); 

   }}

   if( rows.size()>0)
   for(int i=middleIndex-1;i>=0;i--) {
	   if(x.compareTo((Comparable) (rows.get(i)).get(columnName))==0) {
		   if(i==0) {
			   b[1]=true;
		   }
		 search.add(rows.get(i)); 
	   }
	   else
		   break;
	   
   }
   b[2]=keyFound;
   return b;
}

public void search(String columnName, Object value, List<Hashtable<String, Object>> search) {
	for(int i=0;i<rows.size();i++) {
		if(rows.get(i).get(columnName).equals(value))
			search.add(rows.get(i));
	}	
}
public  int getPropValues() throws IOException {
	int result = 0;
	InputStream inputStream = null;
	try {
		Properties prop = new Properties();
		String propFileName = "config.properties";

		inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

		if (inputStream != null) {
			prop.load(inputStream);
		} else {
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		}

		// get the property value and print it out
		String MaximumRowsCountinPage = prop.getProperty("MaximumRowsCountinPage");
		
		result =Integer.parseInt(MaximumRowsCountinPage) ;
		//System.out.println(result );
	} catch (Exception e) {
		System.out.println("Exception: " + e);
	} finally {
		inputStream.close();
	}
	
	return result;
}
public int insertIntoPage(String pageName,Hashtable<String,Object> htblColNameValue,String key) throws DBAppException, IOException {
	int startIndex=0;
	int leftIndex = 0;
    int rightIndex = rows.size() - 1;
    Comparable upperrange=null;
    int middleIndex = 0;
    Comparable<Comparable> x = (Comparable<Comparable>)htblColNameValue.get(key);
    if(rows.size()==0) {
    	pagePointer p=new pagePointer(0,pageName);
    	row r=new row(htblColNameValue, p);
    	rows.add(htblColNameValue);
    }   
    else {
    while(leftIndex <= rightIndex)
    {
        middleIndex = ((leftIndex + rightIndex)) / 2;       
upperrange=(Comparable) (rows.get(middleIndex)).get(key);
        if (x.compareTo(upperrange)<0)
            rightIndex = middleIndex-1;
        else if(x.compareTo(upperrange)>0||(x.compareTo(upperrange)==0))
            leftIndex = middleIndex +1;      	
    }
    pagePointer p=new pagePointer(0,pageName);
	row r=new row(htblColNameValue, p);
    rows.add(leftIndex, htblColNameValue);
    startIndex=leftIndex;
    }
    return startIndex;}

public boolean[] deleteFromPage(Hashtable<String, Object> htblColNameValue, String key) {

	Set<String> keys = htblColNameValue.keySet();
	boolean[] b= {false,false,false};
	boolean keyFound=false;
	int middleIndex2;
	int leftIndex = 0;
    int rightIndex = rows.size() - 1;
    Comparable middle=null;
    int middleIndex = 0;
    Comparable<Comparable> x = (Comparable<Comparable>)htblColNameValue.get(key);
    while(leftIndex <= rightIndex)
    { middleIndex = ((leftIndex + rightIndex)) / 2;       
      middle=(Comparable) (rows.get(middleIndex)).get(key); 
        if (x.compareTo(middle)<0)
            rightIndex = middleIndex-1;
        else if(x.compareTo(middle)>0)
            leftIndex = middleIndex +1;
        else {
        
        	keyFound=true;
        	if(middleIndex==0)
        		b[1]=true;
        	if(checkHashtable(keys, htblColNameValue, rows.get(middleIndex))) {
        	b[0]=true;
            	rightIndex--;
            	rows.remove(middleIndex);
//            System.out.println(rows.size());
            	}
       
            		break;
 	      	
        } 	
    }

    middleIndex2=(middleIndex);
   if(keyFound==true && rows.size()>=middleIndex+1) {
    middle=(Comparable) (rows.get(middleIndex)).get(key); 
   while(x.compareTo(middle)==0 && rows.size()>=middleIndex+1) {
		if(checkHashtable(keys, htblColNameValue, rows.get(middleIndex))) { 
	   rows.remove(middleIndex); }
		else 
			middleIndex++;
		if(rows.size()>=middleIndex+1)
		 middle=(Comparable) (rows.get(middleIndex)).get(key); 
		else
			break;
   }}

   if( rows.size()>0)
   for(int i=middleIndex2-1;i>=0;i--) {
	  
	   
	   if(x.compareTo((Comparable) (rows.get(i)).get(key))==0) {
		   
		   if(i==0) {
			 
			   b[1]=true;
		   }
	   if(checkHashtable(keys, htblColNameValue, rows.get(i))) {
		   rows.remove(i); 
	   }
	 
	   }
	   else
		   break;
	   
   }
   
   
    
  
   b[2]=keyFound;
   return b;}
//
//
public boolean updatePage(Hashtable<String, Object> htblColNameValue,Comparable keyValue, String key, Hashtable<String, String> bptree, Hashtable<String, String> rtree, int use, String strTableName2) throws IOException {
	Set<String> keys = htblColNameValue.keySet();
	int middleIndex2;
	int leftIndex = 0;
    int rightIndex = rows.size() - 1;
    int found=-1;
    boolean continu=true;
    Comparable middle=null;
    int middleIndex = 0;
    Comparable<Comparable> x = (keyValue);
    while(leftIndex <= rightIndex)
    { middleIndex = ((leftIndex + rightIndex)) / 2;       
      middle=(Comparable) (rows.get(middleIndex)).get(key); 
        if (x.compareTo(middle)<0)
            rightIndex = middleIndex-1;
        else if(x.compareTo(middle)>0)
            leftIndex = middleIndex +1;
        else {
        
       
        	found= middleIndex;
 	      	break;
        } 	
    }
   // System.out.println(found);
   
int i=found;
//System.out.println(i+"iiiiii");
//System.out.println( rows.get(0).get(key));
//System.out.println(x);
if(i>=0)
while(i<rows.size()&&((Comparable) rows.get(i).get(key)).compareTo(x)==0) {
	if(keyValue.getClass().toString().equals("class T_221B.polygon")) {
		if(((polygon)keyValue).toString().equals(((polygon)rows.get(i).get(key)).toString())) {
	Set Keys=htblColNameValue.keySet();
	//System.out.println(keys.size());
	for(String key1:keys) {
		//System.out.println("in");
		//System.out.println(key1);
		//System.out.println(htblColNameValue.get(key1));
		
		LocalDateTime l=DBApp.CurrentDate();
		if(bptree.containsKey(key1)) {
			
			BPTree b2=null;
			b2=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b2);
			b2.delete(strTableName2+key1,(Comparable) rows.get(i).get(key1), new Ref(use,i));
			DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");
			//System.out.println(htblColNameValue.get(key1));
			b2.insert(strTableName2+key1, (Comparable)htblColNameValue.get(key1), new Ref(use,i));
			DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");	
		}
		if(rtree.containsKey(key1)) {
			RTree b2=null;
			b2=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b2);
			b2.delete(strTableName2+key1,(Comparable) rows.get(i).get(key1), new Ref(use,i));
			DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");
			//System.out.println(htblColNameValue.get(key1));
			b2.insert(strTableName2+key1, (Comparable)htblColNameValue.get(key1), new Ref(use,i));
			DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");	
		}
		rows.get(i).put(key1, htblColNameValue.get(key1));
	}}}else {
		Set Keys=htblColNameValue.keySet();
		//System.out.println(keys.size());
		for(String key1:keys) {
			//System.out.println("in");
			//System.out.println(key1);
			//System.out.println(htblColNameValue.get(key1));
			
			LocalDateTime l=DBApp.CurrentDate();
			if(bptree.containsKey(key1)) {
				
				BPTree b2=null;
				b2=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b2);
				b2.delete(strTableName2+key1,(Comparable) rows.get(i).get(key1), new Ref(use,i));
				DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");
				//System.out.println(htblColNameValue.get(key1));
				b2.insert(strTableName2+key1, (Comparable)htblColNameValue.get(key1), new Ref(use,i));
				DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");	
			}
			if(rtree.containsKey(key1)) {
				RTree b2=null;
				b2=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b2);
				b2.delete(strTableName2+key1,(Comparable) rows.get(i).get(key1), new Ref(use,i));
				DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");
				//System.out.println(htblColNameValue.get(key1));
				b2.insert(strTableName2+key1, (Comparable)htblColNameValue.get(key1), new Ref(use,i));
				DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");	
			}
			rows.get(i).put(key1, htblColNameValue.get(key1));
	}}
	java.util.Date d=new Date(); 
	rows.get(i).put("TouchDate",d);
	i++;
}
i=0;
if(found>0) {
i=found-1;
while(i>=0&&((Comparable) rows.get(i).get(key)).compareTo(x)==0) {
	Set Keys=htblColNameValue.keySet();
	for(String key1:keys) {
		//System.out.println(key1);
		//System.out.println(htblColNameValue.get(key1));
		
if(bptree.containsKey(key1)) {
			
			BPTree b2=null;
			b2=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b2);
			b2.delete(strTableName2+key1,(Comparable) rows.get(i).get(key1), new Ref(use,i));
			DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");
			//System.out.println(htblColNameValue.get(key1));
			b2.insert(strTableName2+key1, (Comparable)htblColNameValue.get(key1), new Ref(use,i));
			DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");	
		}
		if(rtree.containsKey(key1)) {
			RTree b2=null;
			b2=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b2);
			b2.delete(strTableName2+key1,(Comparable) rows.get(i).get(key1), new Ref(use,i));
			DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");
			//System.out.println(htblColNameValue.get(key1));
			b2.insert(strTableName2+key1, (Comparable)htblColNameValue.get(key1), new Ref(use,i));
			DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");	
		}
		rows.get(i).put(key1, htblColNameValue.get(key1));
		
		
	}
	java.util.Date d=new Date(); 
	rows.get(i).put("TouchDate",d);
	i--;
}}

if(i>0) {
	continu=false;
}
return continu;


}
public void searchInPageLess(Vector<Hashtable<String, Object>> search, Comparable x, String columnName) {
	int middleIndex2;
	int leftIndex = 0;
    int rightIndex = rows.size() - 1;
    Comparable middle=null;
    int middleIndex = 0;
    while(leftIndex <= rightIndex)
    { middleIndex = ((leftIndex + rightIndex)) / 2;       
      middle=(Comparable) (rows.get(middleIndex)).get(columnName); 
        if (x.compareTo(middle)<0)
            rightIndex = middleIndex-1;
        else if(x.compareTo(middle)>0)
            leftIndex = middleIndex +1;
        else {
            	rightIndex--;
	break;
 	      	
        } 	
    }
    middleIndex2=(middleIndex-1);
    if(middleIndex2>=0)
    	for(int i=0;i<=middleIndex2;i++) {
    		if(x.compareTo(rows.get(i).get(columnName))>0)	
    			search.add(rows.get(i));
    		else
    			System.out.println(rows.get(i));
    	}
    for(int i=middleIndex2+1;i<rows.size();i++) {
		if(x.compareTo(rows.get(i).get(columnName))>0)	
			search.add(rows.get(i));
		else
			break;}
    
  }
public void searchInPageLessOrEqual(Vector<Hashtable<String, Object>> search, Comparable x, String columnName) {
	int middleIndex2;
	int leftIndex = 0;
    int rightIndex = rows.size() - 1;
    boolean found=false;
    Comparable middle=null;
    int middleIndex = 0;
    while(leftIndex <= rightIndex)
    { middleIndex = ((leftIndex + rightIndex)) / 2;       
      middle=(Comparable) (rows.get(middleIndex)).get(columnName); 
        if (x.compareTo(middle)<0)
            rightIndex = middleIndex-1;
        else if(x.compareTo(middle)>0)
            leftIndex = middleIndex +1;
        else {
        	
            	rightIndex--;
	break;
 	      	
        } 	
    }
    middleIndex2=(middleIndex);

    if(middleIndex2>=0)
    for(int i=0;i<middleIndex2;i++) {
		if(x.compareTo(rows.get(i).get(columnName))>=0)	
			search.add(rows.get(i));
	}
    for(int i=middleIndex2;i<rows.size();i++) {
		if(x.compareTo(rows.get(i).get(columnName))>=0)	
			search.add(rows.get(i));
		else
			break;
	}
	
}
public boolean[] searchMoreOrEqual(Vector<Hashtable<String, Object>> search, Comparable x, String columnName) {

	boolean[] b= {false,false,false};
	boolean keyFound=false;
	int middleIndex2;
	int leftIndex = 0;
    int rightIndex = rows.size() - 1;
    Comparable middle=null;
    int middleIndex = 0;
    while(leftIndex <= rightIndex)
    { middleIndex = ((leftIndex + rightIndex)) / 2;       
      middle=(Comparable) (rows.get(middleIndex)).get(columnName); 
        if (x.compareTo(middle)<0)
            rightIndex = middleIndex-1;
        else if(x.compareTo(middle)>0)
            leftIndex = middleIndex +1;
        else {
        
        	keyFound=true;
        	if(middleIndex==0)
        		b[1]=true;
        	
        	b[0]=true;
            	rightIndex--;
            	search.add(rows.get(middleIndex));
	break;
 	      	
        } 	
    }
  
//System.out.println(middleIndex);
    middleIndex2=(middleIndex-1);
   if(keyFound==true && rows.size()>middleIndex2) {
   while(x.compareTo(middle)==0 && middleIndex2>=0) {
	   middle=(Comparable) (rows.get(middleIndex2)).get(columnName); 
	   search.add(rows.get(middleIndex2)); 
	   if(middleIndex2==0) {
		   b[1]=true;
	   }
			 middleIndex2--; 

   }}

  
   for(int i=middleIndex+1;i<rows.size();i++) {
	   if(x.compareTo((Comparable) (rows.get(i)).get(columnName))<=0) {
		 search.add(rows.get(i)); 
	   }
	   else
		   break;
   }
   b[2]=keyFound;
   return b;
}
   
   


public void searchMore(Vector<Hashtable<String, Object>> search, Comparable x, String columnName) {
	int middleIndex2;
	int leftIndex = 0;
    int rightIndex = rows.size() - 1;
    Comparable middle=null;
    int middleIndex = 0;
    while(leftIndex <= rightIndex)
    { middleIndex = ((leftIndex + rightIndex)) / 2;       
      middle=(Comparable) (rows.get(middleIndex)).get(columnName); 
        if (x.compareTo(middle)<0)
            rightIndex = middleIndex-1;
        else if(x.compareTo(middle)>0)
            leftIndex = middleIndex +1;
        else {
            	rightIndex--;
	break;
 	      	
        } 	
    }
    middleIndex2=(middleIndex);
    if(middleIndex2<rows.size())
    	for(int i=middleIndex2;i<rows.size();i++) {
    		if(x.compareTo(rows.get(i).get(columnName))<0)	
    			search.add(rows.get(i));
    	}
  }}