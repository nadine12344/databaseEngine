
//allrows to be updated
//select test and do for bpus tree

package T_221B;
import java.awt.Checkbox;
import java.awt.Dimension;
import java.awt.LinearGradientPaint;
import java.awt.Polygon;
import java.awt.color.CMMException;
import java.awt.image.DataBufferDouble;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.print.DocFlavor;
import javax.sound.sampled.Line;
import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;


public class DBApp extends Properties {
	//get from DBApp.config
	int nodeSize;

	//static Vector <Table> tables=new Vector<>();
public DBApp() throws IOException {

	init();
}
	public void init( ) throws IOException {
		nodeSize=getPropValues();
		
	
	}
	public  int getPropValues() throws IOException{
		try {
		FileReader reader=new FileReader("config/DBApp.properties");  
	    Properties p=new Properties();  
	    p.load(reader);   
	   return Integer.parseInt(p.getProperty("NodeSize")); }
		catch(IOException e) {
			System.out.println(e.getMessage());
		}
		return -1;
		
	}
	
	// this does whatever initialization you would like // or leave it empty if there is no code you want to
    // execute at application startup
	
public  static void main (String []args ) throws Exception  {  
		
		

		DBApp dbApp = new DBApp( );
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>( ); 
		htblColNameType.put("idd", "java.lang.Double"); 
		htblColNameType.put("namet", "java.lang.String"); 
		htblColNameType.put("gpa", "java.lang.Double"); 
		htblColNameType.put("TouchDate", "java.util.Date");
		//first we create a table
		dbApp.createTable( "Pooofgfjj", "idd", htblColNameType ); 
		//loop for insertion
		for(int i=0;i<=10;i++) {
			Hashtable h10 = new Hashtable( ); 
			h10.put("idd", new Double(i+0.1)); 
			//System.out.println("continueeeeeeeeed"+i);
			h10.put("namet", new String("salma"+i) ); 
			h10.put("gpa", new Double( 0.95 ) ); 
		dbApp.insertIntoTable("Pooofgfjj", h10);
		}
		Hashtable h10 = new Hashtable( ); 
		h10.put("idd", new Double(0.1)); 
		//System.out.println("continueeeeeeeeed"+3);
		h10.put("namet", new String("salma"+0) ); 
		//h10.put("gpa", new Double(9.95 ) ); 
//dbApp.updateTable("Pooofgfjj","10.1", h10);
		dbApp.deleteFromTable("Pooofgfjj", h10);
		//dbApp.createBTreeIndex("Pooofgfjj","namet");

	//print of tree
//		BPTree b=null;
//		b=(BPTree) dbApp.deserialize("data/Pooofgfjjiddtree.class", b);
//		System.out.println(b.toString());
//		Vector<String> v=(b.searchMore(-9.9));
//		for(int i=0;i<v.size();i++) {
//			Ref2 r2=null;
//			r2=(Ref2) DBApp.deserialize(v.get(i), r2);
//			System.out.println(v.get(i));
//			for(int j=0;j<r2.ref.size();j++) {
//				System.out.println(r2.ref.get(j).getPage());
//				System.out.println(r2.ref.get(j).getIndexInPage());
//			}
//			
//		}
//		hashtablePage h=null;
//		h=(hashtablePage) deserialize("data/PooofgfjjiddtreePages"+".class", h);
//		  Set<Integer> keys1 = h.h.keySet();
//		  for(Integer key: keys1){
//			  System.out.println("page"+key);
//			  Set<String> keys2 = h.h.get(key).keySet();
//	
//			  for(String key2: keys2){
//				  System.out.println("ref"+key2);
//				  System.out.println( "count"+h.h.get(key).get(key2));
//				  
//			  }
//		  }
//		
//			BPTree b2=null;
//			b2=(BPTree) dbApp.deserialize("data/Pooofgfjjnamettree.class", b2);
//			System.out.println(b2.toString());
//			Vector<String> v2=(b2.searchMore("a"));
//			for(int i=0;i<v2.size();i++) {
//				Ref2 r2=null;
//				r2=(Ref2) DBApp.deserialize(v2.get(i), r2);
//				System.out.println(v2.get(i));
//				for(int j=0;j<r2.ref.size();j++) {
//					System.out.println(r2.ref.get(j).getPage());
//					System.out.println(r2.ref.get(j).getIndexInPage());
//				}
//				
//			}
//			hashtablePage h2=null;
//			h2=(hashtablePage) deserialize("data/PooofgfjjnamettreePages"+".class", h2);
//			  Set<Integer> keys12 = h2.h.keySet();
//			  for(Integer key: keys12){
//				  System.out.println("page"+key);
//				  Set<String> keys2 = h2.h.get(key).keySet();
//		
//				  for(String key2: keys2){
//					  System.out.println("ref"+key2);
//					  System.out.println( "count"+h2.h.get(key).get(key2));
//					  
//				  }
//			  }
//////			  
	Table t=null;
	t=(Table) deserialize("data/Pooofgfjj.class", t);
	//System.out.println(getType("pools", "P"));
	System.out.println(t.pages.size());
			for(int i1=0;i1<t.pages.size();i1++) {
				page ne=null;
						ne=(page) deserialize(t.pages.get(i1), ne);
						System.out.println(ne.rows.size());
				for(int j=0;j<ne.rows.size();j++) {
					
					System.out.println(ne.rows.get(j)+"      "+j);
				}
				//System.out.println(b.getIndexForInsert(0, 5).getPage()+"        "+b.getIndexForInsert(0, 5).getIndexInPage());
	
	}
////			System.out.println("here1");
//			SQLTerm[] arrSQLTerms;
//			arrSQLTerms = new SQLTerm[2]; 
//			arrSQLTerms[0]=new SQLTerm("Pooofgfjj","namet","=","salma0");
//			arrSQLTerms[1]=new SQLTerm("Pooofgfjj","idt","=",new Integer( 0 ));
//			
//
//			String[]strarrOperators = new String[1];
//			strarrOperators[0] = "XOR";
//			// select * from Student where name = â€œJohn Noorâ€� or gpa = 1.5;
//			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
//			System.out.println(resultSet.hasNext());
//			while(resultSet.hasNext()) {
//		         Object element = resultSet.next();
//		         System.out.println(element + " ");
//		      }
//			System.out.println("here");
//			Vector v44=binarySearchLess("Pooofgfjj","idt",100);
//			for(int i=0;i<v44.size();i++)
//				System.out.println(v44.get(i));

	}
		private static Vector<Hashtable<String, Object>> binarySearchEqual(String tableName,String columnName,Object value) {
			Vector<Hashtable<String, Object>> search=new Vector<>();
			Table t=null;
			t=(Table) deserialize("data/"+tableName+".class", t);
			t.searchInTableBinary(search,columnName,value);
			return search;
	}
		
		public boolean checkHashtable(Set<String> keys,Hashtable<String,Object> h1,Hashtable<String,Object> h2) {
			for(String key: keys){
				if(!(((Comparable) h1.get(key)).compareTo((Comparable)h2.get(key))==0)) {
					return false;
				}
			}
			return true;
		}
		
		public boolean isTableIndexed(String TableName, String ColumnName) throws IOException {
			BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
			   String row;
			   while ((row = csvReader.readLine()) != null) {
			       String[] data = row.split(",");
			   
			       if(data[0].contentEquals(TableName)) {
			    	   if(data[4]=="True") {
			    		  // System.out.println("dakhal");
			    		   //System.out.println(data[4]);
			    		 
			    			   return true;
			    		   
			    	   }
			       }
			   }
			return false;
		}
		
		public static ArrayList<String> allrowsTable() throws IOException {
			BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
			String l=csvReader.readLine();
			ArrayList<String> s=new ArrayList<>();
			int i=0;
			while (l!=null) {
			    StringTokenizer st=new StringTokenizer(l,",");
			    String table=st.nextToken();
			    s.add(table);
			    s.add(st.nextToken());
			    s.add(st.nextToken());
			    s.add(st.nextToken());
			    s.add(st.nextToken());
			    l=csvReader.readLine();
			   
			}
			 csvReader.close();
			 return s;}
		private static Vector<Hashtable<String, Object>> binarySearch(String tableName,String columnName,Object value, String Op) {
			Vector<Hashtable<String, Object>> search=new Vector<>();
			Table t=null;
			t=(Table) deserialize("data/"+tableName+".class", t);
			if(Op=="=")
				search.addAll(binarySearchEqual(tableName, columnName, value));
			else if(Op==">=")
				search.addAll(binarySearchMoreOrEqual(tableName, columnName, value));
			else if(Op==">")
				search.addAll(binarySearchMore(tableName, columnName, value));
			else if(Op=="<=")
				search.addAll(binarySearchLessOrEqual(tableName, columnName, value));
			else if(Op=="<")
				search.addAll(binarySearchLess(tableName, columnName, value));
			
			return search;
	}
		private static Vector<Hashtable<String, Object>> binarySearchLess(String tableName,
				String columnName, Object value) {
			Vector<Hashtable<String, Object>> search=new Vector<>();
			Table t=null;
			t=(Table) deserialize("data/"+tableName+".class", t);
			t.searchInTableBinaryLess(search,columnName,value);
			return search;			
		}
		private static Vector<Hashtable<String, Object>> binarySearchLessOrEqual(String tableName,
				String columnName, Object value) {
			Vector<Hashtable<String, Object>> search=new Vector<>();
			Table t=null;
			t=(Table) deserialize("data/"+tableName+".class", t);
			t.searchInTableBinaryLessOrEqual(search,columnName,value);
			return search;	
		}
		private static Vector<  Hashtable<String, Object>> binarySearchMore(String tableName,
				String columnName, Object value) {
			Vector<Hashtable<String, Object>> search=new Vector<>();
			Table t=null;
			t=(Table) deserialize("data/"+tableName+".class", t);
			t.searchInTableBinaryMore(search,columnName,value);
			return search;
		
		}
		private static Vector< Hashtable<String, Object>> binarySearchMoreOrEqual(String tableName,
				String columnName, Object value) {
			Vector<Hashtable<String, Object>> search=new Vector<>();
			Table t=null;
			t=(Table) deserialize("data/"+tableName+".class", t);
			t.searchInTableBinaryMoreOrEqual(search,columnName,value);
			return search;
		}
	
		public static boolean isIndexed(String TableName, String ColumnName) throws IOException {
			//return true if the column is indexed 
			ArrayList<String> rows=rows(TableName);
			boolean isIndexed=Indexed(rows, ColumnName);
			   if(isIndexed)
				   return true;
				   else
			return false;
		}
private static boolean Indexed(ArrayList<String> rows, String columnName) {
	for(int i=4;i<rows.size();i=i+5) {
		//System.out.println("row"+rows.get(i-3));
		if(rows.get(i-3).equals(columnName)) {
			if(rows.get(i).equals("True"))
			return true;
			else
				return
						false;
		}}
		return false;
		}
//		public boolean isTableIndexed2(String TableName, String ColumnName) throws IOException {
//			BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
//			   String row;
//			   while ((row = csvReader.readLine()) != null) {
//			       String[] data = row.split(",");
//			   
//			       if(data[0].contentEquals(TableName)) {
//			    	   if(data[4]=="true") {
//			    		  // System.out.println("dakhal");
//			    		   //System.out.println(data[4]);
//			    		 
//			    			   return true;
//			    		   
//			    	   }
//			       }
//			   }
//			return false;
//		}
		public boolean checkClusteringKey(String TableName, String ColumnName) throws IOException {
			//checks if col is clustering key
			 BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
			   String row;
			   while ((row = csvReader.readLine()) != null) {
			       String[] data = row.split(",");
			   
			       if(data[0].contentEquals(TableName)) {
			    	   if(ColumnName.equals(data[1])) {
			    
			    		   if(data[3].equals("True")) {
			    
			    			   return true;
			    		   }
			    	   }
			       }
			   }
			return false;
		}
		public SQLTerm[] priority(SQLTerm[] arrSQLTerms) throws IOException {
			SQLTerm [] orderedTerms=new SQLTerm [arrSQLTerms.length];
			int j=0;
			for (int i=0;i<arrSQLTerms.length;i++) {
			
				
			if(checkClusteringKey(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName)) {
				orderedTerms[j]=arrSQLTerms[i];
				j++;
			}
		}
			for (int i=0;i<arrSQLTerms.length;i++) {
			if(!checkClusteringKey(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName)) {
				orderedTerms[j]=arrSQLTerms[i];
			}
		}
return orderedTerms;
		}
		public Vector<Hashtable<String,Object>> ANDoperation (Vector<Hashtable<String,Object>> a, Vector<Hashtable<String,Object>> b){
			Vector<Hashtable<String,Object>> c= new Vector<Hashtable<String,Object>>();
			for(int i=0;i<a.size();i++) {
				for(int j=0;j<b.size();j++) {
					if(a.get(i).equals(b.get(j))) {
						b.remove(j);
						c.add(a.get(i));
						break;
					}
				}
			}
			
			return c;
			
		}
		public Vector<Hashtable<String,Object>> XORoperation (Vector<Hashtable<String,Object>> a, Vector<Hashtable<String,Object>> b){
			Vector<Hashtable<String,Object>> c=new Vector<Hashtable<String,Object>>();
			boolean flag=true;
			for(int i=0;i<a.size();i++) {
				flag=true;
				for(int j=0;j<b.size();j++) {
					if((a.get(i).equals(b.get(j)))) {
						b.remove(j);
					
						flag=false;
						break;
					}
					
				}
				if (flag){
		
					c.add(a.get(i));
				}
			}
			c.addAll(b);
			return c;
			
		}
		public Vector<Hashtable<String,Object>> ORoperation (Vector<Hashtable<String,Object>> a, Vector<Hashtable<String,Object>> b){
			Vector<Hashtable<String,Object>> c=new Vector<Hashtable<String,Object>>();
			c=a;
			boolean flag=false;
			for(int i=0;i<b.size();i++) {
				for(int j=0;j<a.size();j++) {
					if((b.get(i).equals(a.get(j)))) {
						//System.out.println("equl");
						b.remove(i);
						i--;
						break;
					
					}
				}
			}
			//System.out.println("bbbbbb"+b.size());
			c.addAll(b);
			return c;
			
			
		}

		
		public Vector<Hashtable<String,Object>> linearSearch(String tableName, String columnName, Object value,String op){
			//System.out.println("insandymsandymf");
			Vector<Hashtable<String,Object>> list=new Vector<Hashtable<String,Object>>();
			try {
				File f=new File("data/"+tableName+".class");
				if(f.exists()) {
				Table t=null;
				t=(Table) deserialize("data/"+tableName+".class", t);
							for(int j=0;j<t.pages.size();j++) {
								page l=null;
										l=(page) deserialize(t.pages.get(j), l);
										Vector row= l.rows;
										for(int k=0;k<row.size();k++) {
											Hashtable<String,Object> h =(Hashtable<String,Object>)row.get(k);
											Comparable x=(Comparable)h.get(columnName);
											if(op.contentEquals("=")) {
												//System.out.println("innnnnn");
											if(x.compareTo(value)==0) {
												//System.out.println("innnnnnddd");
												list.add(h);
											}
											}
											if(op.contentEquals(">")) {
												if(x.compareTo(value)>0) {
													list.add(h);
												}
											}
											if(op.contentEquals("<")) {
												if(x.compareTo(value)<0) {
													list.add(h);
												}
											}
											if(op.contentEquals("<=")) {
												if(x.compareTo(value)<=0) {
													list.add(h);
												}
											}
											
											if(op.contentEquals(">=")) {
												if(x.compareTo(value)>=0) {
													list.add(h);
												}
											}
											
											
										}
							}
							//System.out.println(list.size());
							return list;}}
			
			catch(Exception e) {
		System.out.println(e.getMessage());}
			return list;	
							
			
								
		}
private static Vector<Hashtable<String, Object>> indexSearch(String _strTableName, String _strColumnName,
				Object _objValue, String _strOperator) {
			BPTree b=null;
			RTree r=null;
			Vector <Hashtable<String, Object>> h=new Vector<>();
			if(_objValue.getClass().toString().equals("class java.awt.Polygon")) {
				Polygon pl=(Polygon)_objValue;
				polygon pr=new polygon(pl.xpoints, pl.ypoints, (pl.npoints));
				r=(RTree) deserialize("data/"+_strTableName+_strColumnName+"tree.class", r);
				Table t=null;
				t=(Table) deserialize("data/"+_strTableName+".class", t);
				Vector<String> a=new Vector<>();
				if(_strOperator=="=") {
					
					a.add(r.search( pr));}
				else if(_strOperator==">")
					a.addAll(r.searchMore( pr));
				else if(_strOperator==">=")
					a.addAll(r.searchMoreOrEqual( pr));
				else if(_strOperator=="<")
					a.addAll(r.searchLess( pr));
				else if(_strOperator=="<=")
					a.addAll(r.searchLessOrEqual( pr));
				
		for(int i=0;i<a.size();i++) {
			if(!(a.get(i)==null)) {
			Ref2 r2=null;
			r2=(Ref2) deserialize(a.get(i), r2);
			for(int j=0;j<r2.ref.size();j++) {
				page p=null;
				p=(page) deserialize(t.pages.get(r2.ref.get(j).getPage()), p);
				h.add(p.rows.get(r2.ref.get(j).getIndexInPage()));
				
			}}
		}}
			else {
			b=(BPTree) deserialize("data/"+_strTableName+_strColumnName+"tree.class", b);
			Table t=null;
			t=(Table) deserialize("data/"+_strTableName+".class", t);
			Vector<String> a=new Vector<>();
			if(_strOperator=="=")
				a.add(b.search((Comparable) _objValue));
			else if(_strOperator==">")
				a.addAll(b.searchMore((Comparable) _objValue));
			else if(_strOperator==">=")
				a.addAll(b.searchMoreOrEqual((Comparable) _objValue));
			else if(_strOperator=="<")
				a.addAll(b.searchLess((Comparable) _objValue));
			else if(_strOperator=="<=")
				a.addAll(b.searchLessOrEqual((Comparable) _objValue));
			
	for(int i=0;i<a.size();i++) {
		if(!(a.get(i)==null)) {
		Ref2 r2=null;
		r2=(Ref2) deserialize(a.get(i), r2);
		for(int j=0;j<r2.ref.size();j++) {
			page p=null;
			p=(page) deserialize(t.pages.get(r2.ref.get(j).getPage()), p);
			h.add(p.rows.get(r2.ref.get(j).getIndexInPage()));
			
		}}
	}}
		return h;	
		}
private void deleteHashtable(List<Hashtable<String, Object>> m, Hashtable<String, Object> hashtable) {
	List<Hashtable<String, Object>> list=new List<>();
	NodeSelect<Hashtable<String, Object>> head=m.head;
	NodeSelect<Hashtable<String, Object>> current=m.head;
	Set<String> keys = hashtable.keySet();
			while(!(current==null)) {
				if(current==head) {
				if(checkHashtable(keys, hashtable, current.data)) {
					  head=head.next;
						current=current.next;
				}
				else {
					list.head=current;
					list.head.next=null;
				}
				
			}else
				if(checkHashtable(keys, hashtable, current.data)) {
						current=current.next;
				}
				else {
					list.add(current.data);
				}
			
		}
			m=list;}
public  void createTable(String strTableName,String strClusteringKeyColumn,Hashtable<String,String> htblColNameType )throws DBAppException, IOException {
	try {
		if(checkTable(htblColNameType)) {
	File f=new File("data/"+strTableName+".class");
	//System.out.println(f.exists());
	if (f.exists()) {
//		System.out.println("table exists");
//		System.out.println("found");
		return;
	}
	else {


	htblColNameType.put("TouchDate", "java.util.Date");
	Table table=new Table(strTableName,strClusteringKeyColumn,htblColNameType);
	serialize(table, "data/"+strTableName+".class");

    //Creating a File object
	
        FileWriter csvWriter = new FileWriter("data/metadata.csv",true);
        Set<String> keys = htblColNameType.keySet();
		for(String key: keys){
			csvWriter.append(strTableName);
            csvWriter.append(",");
            csvWriter.append(key);
            csvWriter.append(",");
            csvWriter.append(htblColNameType.get(key));
            csvWriter.append(",");
            if(key.equals(strClusteringKeyColumn)) {
            	csvWriter.append("True");
                csvWriter.append(",");
            }
            else { 	csvWriter.append("False");
            csvWriter.append(",");
            	
            }
            csvWriter.append("False");
           // csvWriter.append(",");
            csvWriter.append("\n");
        
		}
   
	    csvWriter.flush();
        csvWriter.close();
	}}else{
		throw new DBAppException("invalid data Type");
		}
	}
	
	catch(Exception e) {
		System.out.println(e.getMessage());}
	}
    

public void updateTable (String strTableName, String strKey,Hashtable<String,Object> htblColNameValue ) throws DBAppException, IOException{
	Hashtable<String, Object> h=new Hashtable<>();
	Set<String> keys5 = htblColNameValue.keySet();
	for(String key: keys5){
		if(htblColNameValue.get(key).getClass().toString().equals("class "+"java.awt.Polygon")) {
			Polygon k=(Polygon) htblColNameValue.get(key);
			polygon key2=new polygon(k.xpoints,k.ypoints,k.npoints);
			h.put(key, key2);
			}
		else
		h.put(key, htblColNameValue.get(key));
		}
	//System.out.println("out");
	Vector<Hashtable<String, Object>> search=new Vector<Hashtable<String, Object>>();

	try {
		if(checkType2(htblColNameValue, strTableName)) {
		Table t=null;
		t=(Table) deserialize("data/"+strTableName+".class", t);
		
		//added for index
		ArrayList<String> s=DBApp.rows(strTableName);
		String key=DBApp.key(s);
		String keyType=DBApp.keyType(s);
		Set<String> keys = htblColNameValue.keySet();
		boolean continu=true;
		
			if(t.bptree.containsKey(key)) {

				t.updateBPlusWithkey(h, strTableName,key,keyType,strKey);
				serialize(t,"data/"+ strTableName+".class");
				continu=false;
			}
			else if(t.rtree.containsKey(key)) {
				t.updateRWithkey(h, strTableName,key,keyType,strKey);
				serialize(t,"data/"+ strTableName+".class");
				continu=false;
			}

		if(continu==true) {
			Comparable keyValue = null;
			if(keyType.equals("java.lang.Integer")) {
				keyValue=Integer.parseInt(strKey);
			}
			else 	if(keyType.equals("java.lang.String")) {
				keyValue=(strKey);
			}
			else 	if(keyType.equals("java.lang.Double")) {
				keyValue=(Double.parseDouble(strKey));
			}
			else 	if(keyType.equals("java.lang.Boolean")) {
				keyValue=(Boolean.parseBoolean(strKey));
			}
			else 	if(keyType.equals("java.util.Date")) {
				keyValue=(java.util.Date.parse(strKey));
			}
			else 	if(keyType.equals("java.awt.Polygon")) {
				
				StringTokenizer st = new StringTokenizer(strKey);
				 Vector <String> s2=new Vector();
				 String hh="";
			     while (st.hasMoreTokens()) {
			    	 String stri=st.nextToken(",");
			    	 for(int i=0;i<stri.length();i++) {
			    		 while(i<stri.length()&&stri.charAt(i)!='('&&stri.charAt(i)!=')'&&stri.charAt(i)!=',') {
			    			 hh+=stri.charAt(i);
			    			 i++;
			    			 if(i>=stri.length())
				    			 break;}
			    			 
			    		 if(hh!="")
			    			 s2.add(""+hh);
			    			 hh="";
			    			 
			    		 if(i>=stri.length())
			    			 break;
			    	 }
			    	
			        
			     }
			    
			     
			
			    int[] x=new int[s2.size()/2];
			    int[] y=new int[s2.size()/2];
			     for(int i=0;i<s2.size()/2;i++) {
			    	 int h2=i*2;
			    	 x[i]=Integer.parseInt(s2.get(h2));
			    	 y[i]=Integer.parseInt(s2.get(h2+1));
			     }
			     polygon po=new polygon(x,y,s2.size()/2);
//			     for(int i=0;i<po.xpoints.length;i++) {
//			    	 System.out.println("x"+po.xpoints[i]);
//			    	 System.out.println("y"+po.ypoints[i]);
//			     }
				     
			  

			  
			     keyValue=po;
			}
			ArrayList<String> s3=DBApp.rows(strTableName);
			String key3=DBApp.key(s3);
			
			t.updateTabe(strTableName,keyValue,key3,h);
			
		
			
serialize(t,"data/"+ strTableName+".class");}}else
	throw new DBAppException("wrong type");
		}

		catch(Exception e) {
System.out.println(e.getMessage());}
	
			

}
public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException {
	try {
		Table t=null;
		t=(Table) deserialize("data/"+strTableName+".class", t);
		ArrayList<String> s=rows(strTableName);
		
		String clusteringkey=key(s);
	
		Hashtable<String, Object> h=new Hashtable<>();
		Set<String> keys = htblColNameValue.keySet();
		for(String key: keys){
			if(htblColNameValue.get(key).getClass().toString().equals("class "+"java.awt.Polygon")) {
				Polygon k=(Polygon) htblColNameValue.get(key);
				polygon key2=new polygon(k.xpoints,k.ypoints,k.npoints);
				h.put(key, key2);
				}
			else
			h.put(key, htblColNameValue.get(key));
			}
		java.util.Date d=new java.util.Date();
		h.put("TouchDate",d);	
			
		if(checkType(s, h)) {
				t.insertIntoTable(h,clusteringkey, strTableName);
			serialize(t, "data/"+strTableName+".class");	
}else
	System.out.println("wrong type");}
	catch(DBAppException e) {
		System.out.println(e.getMessage());
	}
}
public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue)throws DBAppException, IOException {
try {
	//System.out.println("here");
	if(checkType2(htblColNameValue, strTableName)) {
	Hashtable<String, Object> h=new Hashtable<>();
Set<String> keys = htblColNameValue.keySet();
for(String key: keys){
	if(htblColNameValue.get(key).getClass().toString().equals("class "+"java.awt.Polygon")) {
		Polygon k=(Polygon) htblColNameValue.get(key);
		polygon key2=new polygon(k.xpoints,k.ypoints,k.npoints);
		h.put(key, key2);
		}
	else
	h.put(key, htblColNameValue.get(key));
	}
//System.out.println("here");
		Table t=null;
		t=(Table) deserialize("data/"+strTableName+".class", t);
		t.pages=new Vector<>();
		int i=0;
		File f=new File("data/"+strTableName+i+".class");
		while(f.exists()){
			t.pages.add("data/"+strTableName+i+".class");
			i++;
			f=new File("data/"+strTableName+i+".class");
		}

		ArrayList<String> s=rows(strTableName);
		String clusteringkey=key(s);
				t.deleteFromTable( h,strTableName);
				serialize(t, "data/"+strTableName+".class");}
	else
		throw new DBAppException("wrong type");
				
				}catch(DBAppException e) {
					System.out.println(e.getMessage());}
				}
public static LocalDateTime CurrentDate() {
	DateTimeFormatter dtf=DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
	LocalDateTime now=LocalDateTime.now();
	return now;
}
public static void serialize(Serializable s,String path) throws IOException {
    try {
       FileOutputStream fileOut =
       new FileOutputStream(path);
       ObjectOutputStream out = new ObjectOutputStream(fileOut);
       out.writeObject(s);
       out.close();
       fileOut.close();
       //System.out.printf("Serialized data is saved in "+path);
    } catch (IOException i) {
       System.out.println(i.getMessage());
    }

}

public static Serializable deserialize(String s,Serializable s2) {
  try {
     FileInputStream fileIn = new FileInputStream(s);
     ObjectInputStream in = new ObjectInputStream(fileIn);
     s2 =  (Serializable) in.readObject();
     in.close();
     fileIn.close();
     return s2;
  } catch (IOException i) {
 System.out.println(i.getMessage());
     return null;
  } catch (ClassNotFoundException c) {
     System.out.println("Employee class not found");
     return null;
  }
}
public static ArrayList<String> rows(String tableName) throws IOException {
	BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
	String l=csvReader.readLine();
	l=csvReader.readLine();
	ArrayList<String> s=new ArrayList<>();
	boolean flag=false;
	int i=0;
	while (l!=null) {
	    StringTokenizer st=new StringTokenizer(l,",");
	    String table=st.nextToken();
	    if(table.equals(tableName)) {
	    	flag=true;
	    s.add(table);
	    s.add(st.nextToken());
	    s.add(st.nextToken());
	    s.add(st.nextToken());
	    s.add(st.nextToken());
	    }
	    else if(!(table.equals(tableName))&& flag==true) {
	    	return s;
	    }
	    l=csvReader.readLine();
	   
	}
	 csvReader.close();
	 return s;}
public static ArrayList<String> allrows() throws IOException {
	BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
	String l=csvReader.readLine();
	ArrayList<String> s=new ArrayList<>();
	int i=0;
	while (l!=null) {
	    StringTokenizer st=new StringTokenizer(l,",");
	    String table=st.nextToken();
	    s.add(table);
	    s.add(st.nextToken());
	    s.add(st.nextToken());
	    s.add(st.nextToken());
	    l=csvReader.readLine();
	   
	}
	 csvReader.close();
	 return s;}
	public static String  key(ArrayList<String> rows) throws IOException {
		
		for(int i=3;i<rows.size();i=i+5)
		if(rows.get(i).equals("True")) {
			return rows.get(i-2);
		}
		return null;
		}
	public static boolean checkType(ArrayList<String> a,Hashtable<String,Object> htblColNameValue) {
		Set<String> keys = htblColNameValue.keySet();
		ArrayList<String> s=new ArrayList<>();
		for(String key: keys){
		s.add(key);
		}
		for(int i=1;i<a.size();i+=5) {
			if(!keys.contains(a.get(i)))
			return false;
			String text=htblColNameValue.get(a.get(i)).getClass().toString();
			String text2=a.get(i+1);
			if(!(text.equals("class "+text2))&& !text.equals("class T_221B.polygon")&& !text2.equals("java.awt.Polygon")) {
			return false;}
		}
		
		return true;	
	}
	public static boolean checkType2(Hashtable<String,Object> htbl, String tableName) throws IOException{
		ArrayList<String> a=rows(tableName);
		Set<String> keys = htbl.keySet();
		for(int i=1;i<a.size();i+=5) {
			if(keys.contains(a.get(i))) {
			String text=htbl.get(a.get(i)).getClass().toString();
			String text2=a.get(i+1);
			if(!(text.equals("class "+text2))&& !text.equals("class T_221B.polygon")&& !text2.equals("java.awt.Polygon")) {
			return false;}
		}}
		
		return true;	

	}
	
	
	
	public static boolean checkTable(Hashtable<String, String>h) {
		Set<String> keys = h.keySet();
		
		for(String key:keys)
			if(!(h.get(key).equals("java.lang.Integer"))&&!(h.get(key).equals("java.awt.Polygon"))&&!(h.get(key).equals("java.lang.Double"))&&!(h.get(key).equals("java.lang.Boolean"))&&!(h.get(key).equals("java.util.Date"))&&!(h.get(key).equals("java.lang.String")))
				return false;
		return true;
	}
	
	
	
	
	
	public static ArrayList<String> allrows2() throws IOException {
		BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
		String l=csvReader.readLine();
		ArrayList<String> s=new ArrayList<>();
		
		int i=0;
		while (l!=null) {
		    StringTokenizer st=new StringTokenizer(l,",");
		    String table=st.nextToken();
		   
		    s.add(table);
		    s.add(st.nextToken());
		    s.add(st.nextToken());
		    s.add(st.nextToken());
		    s.add(st.nextToken());
		    
		    l=csvReader.readLine();
		   
		}
		 csvReader.close();
		 return s;}
	public void createBTreeIndex(String strTableName,String strColName) throws DBAppException, Exception{
		File f44=new File("data/"+strTableName+strColName+"tree"+".class");
		if(!f44.exists()) {
			ArrayList<String> metadata=allrows2();
			metadata=editRow(strTableName, strColName, metadata);
			overWrite(metadata);
		Table table=null;
		BPTree b=new BPTree(nodeSize);
		table=(Table) deserialize("data/"+strTableName+".class", table);
		String type=getType(strTableName,strColName);
		//System.out.println(type);
		if(!type.equals( "java.awt.Polygon")) {
			hashtablePage h=new hashtablePage();
			//System.out.println("llll");
			serialize(h,"data/"+strTableName+strColName+"tree"+"Pages"+".class");
		if(type.equals("java.lang.Integer"))
			b = new BPTree<Integer>(nodeSize);
		else if(type.equals("java.lang.String"))
			b = new BPTree<String>(nodeSize);
		else if(type.equals("java.lang.Double"))
			b = new BPTree<Double>(nodeSize);
		else if(type.equals("java.lang.Boolean")) 
			b = new BPTree<Boolean>(nodeSize);
		else if(type.equals("java.util.Date"))
	 		b = new BPTree<java.util.Date>(nodeSize);
		//need to ask
//		else if(type.equals("java.util.Date"))
//			b = new BPTree<Date>(nodeSize);
		//else if(type.equals("java.awt.Polygon"))   ////hena rtree sa7 ????????
			//b = new BPTree<polygon>(nodeSize);
		int k=0;
		File f=new File("data/"+strTableName+k+".class");
		while(f.exists()){
			page p=null;
			p=(page)deserialize("data/"+strTableName+k+".class",p );
			for(int i=0;i<p.rows.size();i++) {
				Ref r=new Ref(k, i);
				b.insert(strTableName+strColName, (Comparable)p.rows.get(i).get(strColName), r);
			}
			k++;
			f=new File("data/"+strTableName+k+".class");
		}
		serialize(b, "data/"+strTableName+strColName+"tree"+".class");
		table.bptree.put(strColName,strTableName+strColName+"tree");
		serialize(table, "data/"+strTableName+".class");}
		else {
			throw new DBAppException("wrongType");
		}
		}
	}
			private static String getType(String strTableName, String strColName) throws IOException {
	     ArrayList<String>a= rows( strTableName);
	     int j=0;
	    
	     for(int i=1;i<a.size();i+=5) {
	    
	    	if(a.get(i).equals(strColName))
	    		return a.get(i+1);			
	     }
		return null;
	}
			public void createRTreeIndex(String strTableName,String strColName) throws DBAppException, IOException{
				File f44=new File("data/"+strTableName+strColName+"tree"+".class");
				if(!f44.exists()) {
					ArrayList<String> metadata=allrows2();
					metadata=editRow(strTableName, strColName, metadata);
					overWrite(metadata);
				Table table=null;
			RTree b=new RTree(nodeSize);
				table=(Table) deserialize("data/"+strTableName+".class", table);
				String type=getType(strTableName,strColName);
				if(type.equals( "java.awt.Polygon")) {
					hashtablePage h=new hashtablePage();
					//System.out.println("llll");
					serialize(h,"data/"+strTableName+strColName+"tree"+"Pages"+".class");
				
					b = new RTree<polygon>(nodeSize);
				
				//need to ask
//				else if(type.equals("java.util.Date"))
//					b = new BPTree<Date>(nodeSize);
				//else if(type.equals("java.awt.Polygon"))   ////hena rtree sa7 ????????
					//b = new BPTree<polygon>(nodeSize);
				int k=0;
				File f=new File("data/"+strTableName+k+".class");
				while(f.exists()){
					page p=null;
					p=(page)deserialize("data/"+strTableName+k+".class",p );
					for(int i=0;i<p.rows.size();i++) {
						Ref r=new Ref(k, i);
						b.insert(strTableName+strColName, (Comparable)p.rows.get(i).get(strColName), r);
					}
					k++;
					f=new File("data/"+strTableName+k+".class");
				}
				serialize(b, "data/"+strTableName+strColName+"tree"+".class");
				table.rtree.put(strColName,strTableName+strColName+"tree");
				serialize(table, "data/"+strTableName+".class");}
				else {
					throw new DBAppException("wrongType");
				}
				
				
				}
			}
			public static ArrayList<String> editRow(String tableName,String coumnName,ArrayList<String> s) {
				boolean foundtable=false;
				boolean foundcolumn=false;
				for(int i=0;i<s.size()-4;i++) {
					
					if(s.get(i).equals(tableName)) {
					
						foundtable=true;
					}
					
					if(foundtable&& s.get(i+1).contentEquals(coumnName)) {
	
						foundcolumn=true;
						s.remove(i+4);
						s.add(i+4,"True");
						break;
					}
				}
				return s;
			}
public static Vector<String>  tables() throws IOException {
		
		String name="";
		ArrayList<String> a=allrows2(); 
		Vector<String> v=new Vector<>();
		for(int i=0;i<a.size();i=i+5)
			if(!(a.get(i).equals(name))) {
		v.add(a.get(i));
		name=a.get(i);}
		return v;
		}
public static void overWrite(ArrayList<String>a) throws IOException {
	 FileWriter csvWriter = new FileWriter("data/metadata.csv");
     
		for(int i=0;i<a.size();i+=5){
		csvWriter.append(a.get(i));
         csvWriter.append(",");
         csvWriter.append(a.get(i+1));
         csvWriter.append(",");
         csvWriter.append(a.get(i+2));
         csvWriter.append(",");
         csvWriter.append(a.get(i+3));
         csvWriter.append(",");
         csvWriter.append(a.get(i+4));
         csvWriter.append(",");
         csvWriter.append("\n");
     
		}

	    csvWriter.flush();
     csvWriter.close();
}
public static String  keyType(ArrayList<String> rows) throws IOException {
	for(int i=3;i<rows.size();i=i+5)
	if(rows.get(i).equals("True")) {
		return rows.get(i-1);
	}
	return null;
	}
public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)throws DBAppException, IOException{
	Iterator l;
	Vector<Hashtable<String,Object>> hashtables=new Vector<Hashtable<String,Object>>();
	Vector<Hashtable<String,Object>> list=new Vector<Hashtable<String,Object>>();	
	for(int i=0;i<arrSQLTerms.length;i++) {
		//System.out.println(i+"iiiii");
		if(arrSQLTerms[i]._strOperator.equals("!=")||!isIndexed(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName )) {
			if(!checkClusteringKey(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName )) {
				// System.out.println("hereeeeeeeeeeee");
			 list=linearSearch(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName ,arrSQLTerms[i]._objValue,arrSQLTerms[i]._strOperator);
			 if(i==0)
				 hashtables=list;
			 else {
				
				 if(strarrOperators[i-1].contentEquals("AND")) {
						hashtables=ANDoperation(hashtables,list);
					}
					if(strarrOperators[i-1].contentEquals("OR")) {
						//System.out.println("it is or");
						hashtables=ORoperation(hashtables,list);
					}
					if(strarrOperators[i-1].contentEquals("XOR")) {
						hashtables=XORoperation(hashtables,list);
					}
			}}
			else {
				//System.out.println("hereeeeeeeeeeee1");
				 //System.out.println("else");
				list=binarySearch(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName ,arrSQLTerms[i]._objValue,arrSQLTerms[i]._strOperator);
				//System.out.println(list.size()+"size");
				 if(i==0)
					 hashtables=list;
				 else {
					 //System.out.println("else2");
					 if(strarrOperators[i-1].contentEquals("AND")) {
							hashtables=ANDoperation(hashtables,list);
						}
						if(strarrOperators[i-1].contentEquals("OR")) {
							//System.out.println("else2or");
							hashtables=ORoperation(hashtables,list);
						}
						if(strarrOperators[i-1].contentEquals("XOR")) {
							hashtables=XORoperation(hashtables,list);
						}
			}}
		}
		else {
			//System.out.println("hereeeeeeeeeeee2");
			list=indexSearch(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName ,arrSQLTerms[i]._objValue,arrSQLTerms[i]._strOperator);
			 if(i==0)
				 hashtables=list;
			 else {
				 if(strarrOperators[i-1].contentEquals("AND")) {
						hashtables=ANDoperation(hashtables,list);
					}
					if(strarrOperators[i-1].contentEquals("OR")) {
						hashtables=ORoperation(hashtables,list);
					}
					if(strarrOperators[i-1].contentEquals("XOR")) {
						hashtables=XORoperation(hashtables,list);
					}
		}}
	}
	return hashtables.iterator();
	}



}


