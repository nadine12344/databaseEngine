package T_221B;
import java.time.LocalDateTime;
import java.time.chrono.ChronoLocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

import java.lang.*;
import java.awt.*;
import java.time.*;
import java.io.*;
public class Table implements Serializable{
	String strTableName;
	String strClusteringKeyColumn;
	Hashtable<String,String> htblColNameType;
    Vector <String> pages;
    int maxSize;
page current;
Hashtable<String, String> bptree=new Hashtable<>();
Hashtable<String, String> rtree=new Hashtable<>();
public  int getPropValues() throws IOException{
	try {
	FileReader reader=new FileReader("config/DBApp.properties");  
    Properties p=new Properties();  
    p.load(reader);   
   return Integer.parseInt(p.getProperty("MaximumRowsCountinPage")); }
	catch(IOException e) {
		System.out.println(e.getMessage());
	}
	return -1;
	
}

public void searchInTableBinary(Vector<Hashtable<String, Object>> search,String columnName, Object value) {
	if(pages.size()>0) {
		Comparable x;
		if(value.getClass().toString().equals("class java.awt.Polygon")) {
			Polygon v=(Polygon) value;
			polygon po=new polygon(v.xpoints, v.ypoints, v.npoints);
			x=po;}
		else
		x = (Comparable)value;
		int  upperbound=pages.size()-1;
		int lowerbound=0;
		int middlePage=0;
		boolean still=false;
		boolean found=false;
		boolean keyFound=false;
		while(lowerbound < upperbound)
	    {
	middlePage = (lowerbound + upperbound) / 2;
	//System.out.println(middlePage);
	        page m=null;
			m=(page) DBApp.deserialize(pages.get(middlePage), m);
			//System.out.println((m).lessThan( htblColNameValue,key));
	 if ((m).lessThan3( x,columnName))
	            upperbound = middlePage ;
	        else
	            lowerbound = middlePage + 1;
	    }
		
	   int use=upperbound;
	   page usePage=null;
		usePage=(page) DBApp.deserialize(pages.get(use), usePage);
		boolean[] b= usePage.searchInPage( search,x,columnName);
		found=b[0];
		still=b[1];
		keyFound=b[2];
		//System.out.println(still);
		//System.out.println(keyFound);
	if(use>0 && (still==true||keyFound==false)) { 
		for(int i=use-1;i>=0;i--) {
		page p2=null;		
		p2=(page) DBApp.deserialize(pages.get(i), p2);
		for(int j=p2.rows.size()-1;j>=0&&p2.rows.size()>0;j--) {
			if(x.compareTo((Comparable) p2.rows.get(j).get(columnName))==0) {
				
				search.add(p2.rows.get(j));}
				else 
				{break;}	
		}
	
	}
	}}
	
}
public void deleteFromTable(Hashtable<String,Object> htblColNameValue,String name) throws IOException {
	ArrayList<String> s=DBApp.rows(strTableName);
	String key=DBApp.key(s);
	Set<String> keys = htblColNameValue.keySet();
	boolean continu=true;
	if(keys.contains(key)) {
		if(bptree.containsKey(key)&& htblColNameValue.containsKey(key)) {
			deleteBPlusWithkey(htblColNameValue, name,key);
			continu=false;
		}
		else if(rtree.containsKey(key)&& htblColNameValue.containsKey(key)) {
			deleteRWithkey(htblColNameValue, name,key);
			continu=false;
		}
		
	}
	if(continu==true) {
		for(String key1:keys) {
			if(bptree.containsKey(key1) && htblColNameValue.containsKey(key1)) {
				deleteWithoutkeyBplus(htblColNameValue, name,key1);
				continu=false;
				break;
			}
			
		}
	if(continu==true) {
	for(String key1:keys) {
		if(rtree.containsKey(key1)&& htblColNameValue.containsKey(key1)) {
			deleteWithoutkeyRtree(htblColNameValue, name,key1);
			continu=false;
			break;
		}
		
	}}}
	if(continu==true) {
		if(keys.contains(key)) {
				deleteKey(htblColNameValue,name,key);	
		}
		else {
		//int s=pages.size();
	for(int i=0;i<pages.size();i++) {
		page p2=null;
		
		p2=(page) DBApp.deserialize(pages.get(i), p2);
	
		for(int j=0;j<p2.rows.size();j++) {
			if(checkHahtable(keys,htblColNameValue,p2.rows.get(j))) {
				Set<String> keys2 = bptree.keySet();
				for(String key1: keys2){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					b3.delete(strTableName+key1, (Comparable) p2.rows.get(j).get(key1), new Ref(i, j));
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys2r = rtree.keySet();
				for(String key1: keys2r){
					RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					b3.delete(strTableName+key1, (Comparable) p2.rows.get(j).get(key1), new Ref(i, j));
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}
				//System.out.println(p2.rows.size());
				p2.rows.remove(j);
				if(j<p2.rows.size()) {
					Set<String> keys3 = bptree.keySet();
					for(String key1: keys3){
						BPTree b3=null;
						b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
						DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");
						b3.shiftInPageForDelete(strTableName+key1,i,j);
						DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
					}
					Set<String> keys3r = rtree.keySet();
					for(String key1: keys3r){
						RTree b3=null;
						b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
						DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");
						b3.shiftInPageForDelete(strTableName+key1,i,j);
						DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
					}}
				if (p2.rows.size()==0){
					Set<String> keys7 = bptree.keySet();
				for(String key1: keys7){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");
					b3.shiftPagesForDelete(strTableName+key1,i);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys7r = rtree.keySet();
				for(String key1: keys7r){
				RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");
					b3.shiftPagesForDelete(strTableName+key1,i);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}}
				//System.out.println(p2.rows.size());
				
				//current=p2;
				if(p2.rows.size()==0) {

					for(int r=i;r<pages.size()-1;r++) {
						String ss=pages.get(r);
						File f=new File(ss);
						File d=new File(pages.get(r+1));
						d.renameTo(f);
					}
					File f=new File(pages.get(pages.size()-1));
					f.delete();
					pages.remove(pages.size()-1);
					i--;
					break;
				}
			if(p2.rows.size()>0)
				j--;	
			
		}	    
	}if(p2.rows.size()>0)
		DBApp.serialize(p2, pages.get(i));
		//DBApp.serialize(p2, name+i);
	}

	}}
	
}
private void deleteWithoutkeyRtree(Hashtable<String, Object> htblColNameValue, String name, String key) throws IOException {
	//System.out.println("heeereeee");
	Set<String> keys = htblColNameValue.keySet();
	RTree b=null;
	b=(RTree) DBApp.deserialize("data/"+rtree.get(key)+".class", b);
	String v=b.search((Comparable) htblColNameValue.get(key));
	
	//System.out.println(v);
	Vector<Integer>deletedPages=new Vector<>();
	Ref2 r2=null;
	//System.out.println(v);
	if(!(v==null)) {
	r2=(Ref2) DBApp.deserialize(v, r2);
	int i=0;
	while(i<r2.ref.size() && r2.ref.size()>0) {
		int currentPage=(r2.ref.get(i).getPage());
		page p2=null;
		p2=(page) DBApp.deserialize(pages.get(r2.ref.get(i).getPage()), p2);
		//System.out.println(i+" j"+r2.ref.size());
		while (  i<r2.ref.size() && r2.ref.get(i).getPage()==currentPage) {
			
			if(checkHahtable4(keys,htblColNameValue,p2.rows.get(r2.ref.get(i).getIndexInPage()))) {
				//System.out.println("ooooooo");
				if(((polygon)(p2.rows.get(r2.ref.get(i).getIndexInPage())).get(key)).toString().equals(((polygon)htblColNameValue.get(key)).toString())) {
	
				
			//need to delete from all trees
				Set<String> keys2 = bptree.keySet();
				for(String key1: keys2){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					b3.delete(strTableName+key1, (Comparable) p2.rows.get(r2.ref.get(i).getIndexInPage()).get(key1), new Ref(r2.ref.get(i).getPage(), r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys2r = rtree.keySet();
				for(String key1: keys2r){
					RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					b3.delete(strTableName+key1, (Comparable) p2.rows.get(r2.ref.get(i).getIndexInPage()).get(key1), new Ref(r2.ref.get(i).getPage(), r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}
				p2.rows.remove(r2.ref.get(i).getIndexInPage());
				//System.out.println(p2.rows.size());
				if(r2.ref.get(i).getIndexInPage()<p2.rows.size()) {
				Set<String> keys3 = bptree.keySet();
				for(String key1: keys3){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");
					b3.shiftInPageForDelete(strTableName+key1,currentPage,r2.ref.get(i).getIndexInPage());
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys3r = rtree.keySet();
				for(String key1: keys3r){
					RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");
					b3.shiftInPageForDelete(strTableName+key1,currentPage,r2.ref.get(i).getIndexInPage());
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}}
				if (p2.rows.size()==0){
					Set<String> keys7 = bptree.keySet();
				for(String key1: keys7){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");
					b3.shiftPagesForDelete(strTableName+key1,currentPage);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys7r = rtree.keySet();
				for(String key1: keys7r){
				RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");
					b3.shiftPagesForDelete(strTableName+key1,currentPage);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}}
				if(p2.rows.size()==0) {
					for(int r=r2.ref.get(i).getPage();r<pages.size()-1;r++) {
						String ss=pages.get(r);
						File f=new File(ss);
						File d=new File(pages.get(r+1));
						d.renameTo(f);
						}
					File f=new File(pages.get(pages.size()-1));
					f.delete();
					pages.remove(pages.size()-1);
					break;
				}			
			}else if(i<r2.ref.size())
				i++;
			else if(i>r2.ref.size()-1)
				break;
			}
			else if(i<r2.ref.size())
				i++;
			else if(i>r2.ref.size()-1)
				break;
			//System.out.println("i"+i);
				
			
			File f=new File(v);
			if(f.exists())
			r2=(Ref2) DBApp.deserialize(v, r2);
			else break;
			//System.out.println("i"+i);
			
	} 
		
		//System.out.println("out");	
			
		if(p2.rows.size()>0)
		DBApp.serialize(p2, pages.get(currentPage));
		
		File f=new File(v);
		if(f.exists())
		r2=(Ref2) DBApp.deserialize(v, r2);
		else 
			break;
		if(i==r2.ref.size())
			break;
		
	}}
	
	
	//DBApp.serialize(p2, name+i);
	}

private void deleteWithoutkeyBplus(Hashtable<String, Object> htblColNameValue, String name, String key) throws IOException {
	//System.out.println("koko");
	Set<String> keys = htblColNameValue.keySet();
	BPTree b=null;
	b=(BPTree) DBApp.deserialize("data/"+bptree.get(key)+".class", b);
	String v=b.search((Comparable) htblColNameValue.get(key));
	//System.out.println(v);
	Vector<Integer>deletedPages=new Vector<>();
	Ref2 r2=null;
	//System.out.println(v);
	if(!(v==null)) {
	r2=(Ref2) DBApp.deserialize(v, r2);
	int i=0;
	while(i<r2.ref.size() && r2.ref.size()>0) {
		int currentPage=(r2.ref.get(i).getPage());
		page p2=null;
		p2=(page) DBApp.deserialize(pages.get(r2.ref.get(i).getPage()), p2);
		//System.out.println(i+" j"+r2.ref.size());
		while (  i<r2.ref.size() && r2.ref.get(i).getPage()==currentPage) {
//			System.out.println("currentPage"+currentPage);
//			System.out.println(i);
//			System.out.println("size"+r2.ref.size());
//			System.out.println(r2.ref.get(i).getPage());
//			System.out.println(r2.ref.get(i).getIndexInPage());
			if(checkHahtable(keys,htblColNameValue,p2.rows.get(r2.ref.get(i).getIndexInPage()))) {				
			//need to delete from all trees
				Set<String> keys2 = bptree.keySet();
				for(String key1: keys2){
					//System.out.println(key1);
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					b3.delete(strTableName+key1, (Comparable) p2.rows.get(r2.ref.get(i).getIndexInPage()).get(key1), new Ref(r2.ref.get(i).getPage(), r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys2r = rtree.keySet();
				for(String key1: keys2r){
					RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					b3.delete(strTableName+key1, (Comparable) p2.rows.get(r2.ref.get(i).getIndexInPage()).get(key1), new Ref(r2.ref.get(i).getPage(), r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}
				p2.rows.remove(r2.ref.get(i).getIndexInPage());
				//System.out.println(p2.rows.size());
				if(r2.ref.get(i).getIndexInPage()<p2.rows.size()) {
				Set<String> keys3 = bptree.keySet();
				for(String key1: keys3){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");
					b3.shiftInPageForDelete(strTableName+key1,currentPage,r2.ref.get(i).getIndexInPage());
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys3r = rtree.keySet();
				for(String key1: keys3r){
					RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");
					b3.shiftInPageForDelete(strTableName+key1,currentPage,r2.ref.get(i).getIndexInPage());
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}}
				if (p2.rows.size()==0){
					Set<String> keys7 = bptree.keySet();
				for(String key1: keys7){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");
					b3.shiftPagesForDelete(strTableName+key1,currentPage);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys7r = rtree.keySet();
				for(String key1: keys7r){
					RTree b3=null;
						b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
						DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");
						b3.shiftPagesForDelete(strTableName+key1,currentPage);
						DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
					}}
				if(p2.rows.size()==0) {
					for(int r=r2.ref.get(i).getPage();r<pages.size()-1;r++) {
						String ss=pages.get(r);
						File f=new File(ss);
						File d=new File(pages.get(r+1));
						d.renameTo(f);
						}
					File f=new File(pages.get(pages.size()-1));
					f.delete();
					pages.remove(pages.size()-1);
					break;
				}			
			}
			else if(i<r2.ref.size()-1)
				i++;
			else if(i>r2.ref.size()-1)
				break;
			//System.out.println("i"+i);
				
			
			File f=new File(v);
			if(f.exists())
			r2=(Ref2) DBApp.deserialize(v, r2);
			else break;
			//System.out.println("i"+i);
			
	} 
		
		//System.out.println("out");	
			
		if(p2.rows.size()>0)
		DBApp.serialize(p2, pages.get(currentPage));
		
		File f=new File(v);
		if(f.exists())
		r2=(Ref2) DBApp.deserialize(v, r2);
		else 
			break;
		if(i>r2.ref.size())
			break;
		
	}}
	
	}
private void deleteRWithkey(Hashtable<String, Object> htblColNameValue, String name, String key) throws IOException {
	//System.out.println("heeereeee");
	Set<String> keys = htblColNameValue.keySet();
	RTree b=null;
	b=(RTree) DBApp.deserialize("data/"+rtree.get(key)+".class", b);
	String v=b.search((Comparable) htblColNameValue.get(key));
	
	//System.out.println(v);
	Vector<Integer>deletedPages=new Vector<>();
	Ref2 r2=null;
	//System.out.println(v);
	if(!(v==null)) {
	r2=(Ref2) DBApp.deserialize(v, r2);
	int i=0;
	while(i<r2.ref.size() && r2.ref.size()>0) {
		int currentPage=(r2.ref.get(i).getPage());
		page p2=null;
		p2=(page) DBApp.deserialize(pages.get(r2.ref.get(i).getPage()), p2);
		//System.out.println(i+" j"+r2.ref.size());
		while (  i<r2.ref.size() && r2.ref.get(i).getPage()==currentPage) {
			
			if(checkHahtable4(keys,htblColNameValue,p2.rows.get(r2.ref.get(i).getIndexInPage()))) {
				//System.out.println("ooooooo");
				if(((polygon)(p2.rows.get(r2.ref.get(i).getIndexInPage())).get(key)).toString().equals(((polygon)htblColNameValue.get(key)).toString())) {
	
				
			//need to delete from all trees
				Set<String> keys2 = bptree.keySet();
				for(String key1: keys2){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					b3.delete(strTableName+key1, (Comparable) p2.rows.get(r2.ref.get(i).getIndexInPage()).get(key1), new Ref(r2.ref.get(i).getPage(), r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys2r = rtree.keySet();
				for(String key1: keys2r){
					RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					b3.delete(strTableName+key1, (Comparable) p2.rows.get(r2.ref.get(i).getIndexInPage()).get(key1), new Ref(r2.ref.get(i).getPage(), r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}
				p2.rows.remove(r2.ref.get(i).getIndexInPage());
				//System.out.println(p2.rows.size());
				if(r2.ref.get(i).getIndexInPage()<p2.rows.size()) {
				Set<String> keys3 = bptree.keySet();
				for(String key1: keys3){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");
					b3.shiftInPageForDelete(strTableName+key1,currentPage,r2.ref.get(i).getIndexInPage());
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys3r = rtree.keySet();
				for(String key1: keys3r){
					RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");
					b3.shiftInPageForDelete(strTableName+key1,currentPage,r2.ref.get(i).getIndexInPage());
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}}
				if (p2.rows.size()==0){
					Set<String> keys7 = bptree.keySet();
				for(String key1: keys7){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");
					b3.shiftPagesForDelete(strTableName+key1,currentPage);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys7r = rtree.keySet();
				for(String key1: keys7r){
				RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");
					b3.shiftPagesForDelete(strTableName+key1,currentPage);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}}
				if(p2.rows.size()==0) {
					for(int r=r2.ref.get(i).getPage();r<pages.size()-1;r++) {
						String ss=pages.get(r);
						File f=new File(ss);
						File d=new File(pages.get(r+1));
						d.renameTo(f);
						}
					File f=new File(pages.get(pages.size()-1));
					f.delete();
					pages.remove(pages.size()-1);
					break;
				}			
			}else if(i<r2.ref.size())
				i++;
			else if(i>r2.ref.size()-1)
				break;
			}
			else if(i<r2.ref.size())
				i++;
			else if(i>r2.ref.size()-1)
				break;
			//System.out.println("i"+i);
				
			
			File f=new File(v);
			if(f.exists())
			r2=(Ref2) DBApp.deserialize(v, r2);
			else break;
			//System.out.println("i"+i);
			
	} 
		
		//System.out.println("out");	
			
		if(p2.rows.size()>0)
		DBApp.serialize(p2, pages.get(currentPage));
		
		File f=new File(v);
		if(f.exists())
		r2=(Ref2) DBApp.deserialize(v, r2);
		else 
			break;
		if(i==r2.ref.size())
			break;
		
	}}
	
	
	//DBApp.serialize(p2, name+i);
	}

	
	
	

	

	
	
	

private void deleteBPlusWithkey(Hashtable<String, Object> htblColNameValue, String name, String key) throws IOException {
	Set<String> keys = htblColNameValue.keySet();
	BPTree b=null;
	b=(BPTree) DBApp.deserialize("data/"+bptree.get(key)+".class", b);
	String v=b.search((Comparable) htblColNameValue.get(key));
	//System.out.println(v);
	Vector<Integer>deletedPages=new Vector<>();
	Ref2 r2=null;
	//System.out.println(v);
	if(!(v==null)) {
	r2=(Ref2) DBApp.deserialize(v, r2);
	int i=0;
	while(i<r2.ref.size() && r2.ref.size()>0) {
		int currentPage=(r2.ref.get(i).getPage());
		page p2=null;
		p2=(page) DBApp.deserialize(pages.get(r2.ref.get(i).getPage()), p2);
		//System.out.println(i+" j"+r2.ref.size());
		while (  i<r2.ref.size() && r2.ref.get(i).getPage()==currentPage) {
			
			if(checkHahtable(keys,htblColNameValue,p2.rows.get(r2.ref.get(i).getIndexInPage()))) {
			//need to delete from all trees
				Set<String> keys2 = bptree.keySet();
				for(String key1: keys2){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					b3.delete(strTableName+key1, (Comparable) p2.rows.get(r2.ref.get(i).getIndexInPage()).get(key1), new Ref(r2.ref.get(i).getPage(), r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys2r = rtree.keySet();
				for(String key1: keys2r){
					RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					b3.delete(strTableName+key1, (Comparable) p2.rows.get(r2.ref.get(i).getIndexInPage()).get(key1), new Ref(r2.ref.get(i).getPage(), r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}
				p2.rows.remove(r2.ref.get(i).getIndexInPage());
			
				if(r2.ref.get(i).getIndexInPage()<p2.rows.size()) {
				Set<String> keys3 = bptree.keySet();
				for(String key1: keys3){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");
					b3.shiftInPageForDelete(strTableName+key1,currentPage,r2.ref.get(i).getIndexInPage());
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys3r = rtree.keySet();
				for(String key1: keys3r){
					RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");
					b3.shiftInPageForDelete(strTableName+key1,currentPage,r2.ref.get(i).getIndexInPage());
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}}
				if (p2.rows.size()==0){
					Set<String> keys7 = bptree.keySet();
				for(String key1: keys7){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");
					b3.shiftPagesForDelete(strTableName+key1,currentPage);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys7r = rtree.keySet();
				for(String key1: keys7r){
				RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");
					b3.shiftPagesForDelete(strTableName+key1,currentPage);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}}
				if(p2.rows.size()==0) {
					for(int r=r2.ref.get(i).getPage();r<pages.size()-1;r++) {
						String ss=pages.get(r);
						File f=new File(ss);
						File d=new File(pages.get(r+1));
						d.renameTo(f);
						}
					File f=new File(pages.get(pages.size()-1));
					f.delete();
					pages.remove(pages.size()-1);
					break;
				}			
			}
			else if(i<r2.ref.size())
				i++;
			else if(i>r2.ref.size()-1)
				break;
			//System.out.println("i"+i);
				
			
			File f=new File(v);
			if(f.exists())
			r2=(Ref2) DBApp.deserialize(v, r2);
			else break;
			//System.out.println("i"+i);
			
	} 
		
//		System.out.println("out");
//		System.out.println(r2.ref.size());
//		System.out.println("i"+i);
		if(p2.rows.size()>0)
		DBApp.serialize(p2, pages.get(currentPage));
		
		File f=new File(v);
		if(f.exists())
		r2=(Ref2) DBApp.deserialize(v, r2);
		else 
			break;
		if(i==r2.ref.size())
			break;
		
	}}
	
	
	//DBApp.serialize(p2, name+i);
	}


	
private void deleteKey(Hashtable<String, Object> htblColNameValue, String name, String key) throws IOException {
	//System.out.println("hhhhhhh");
	if(pages.size()>0) {
	Set<String> keys = htblColNameValue.keySet();
	Comparable<Comparable> x = (Comparable<Comparable>)htblColNameValue.get(key);
	int  upperbound=pages.size()-1;
	int lowerbound=0;
	int middlePage=0;
	boolean still=false;
	boolean found=false;
	boolean keyFound=false;
	while(lowerbound < upperbound)
    {
middlePage = (lowerbound + upperbound) / 2;
//System.out.println(middlePage);
        page m=null;
		m=(page) DBApp.deserialize(pages.get(middlePage), m);
		//System.out.println((m).lessThan( htblColNameValue,key));
 if ((m).lessThan2( htblColNameValue,key))
            upperbound = middlePage ;
        else
            lowerbound = middlePage + 1;
    }
   int use=upperbound;
   //System.out.println(use);
   page usePage=null;
	usePage=(page) DBApp.deserialize(pages.get(use), usePage);
	boolean[] b= usePage.deleteFromPage(htblColNameValue,key);
		DBApp.serialize(usePage,pages.get(use));
	found=b[0];
	still=b[1];
	keyFound=b[2];
	if(usePage.rows.size()==0) {
		for(int r=use;r<pages.size()-1;r++) {
			String ss=pages.get(r);
			File f=new File(ss);
			File d=new File(pages.get(r+1));
			d.renameTo(f);
		}
		File f=new File(pages.get(pages.size()-1));
		f.delete();
		pages.remove(pages.size()-1);
	}
	//System.out.println(still);
	//System.out.println(keyFound);
if(use>0 && (still==true||keyFound==false)) {
	
	for(int i=use-1;i>=0;i--) {
	page p2=null;		
	p2=(page) DBApp.deserialize(pages.get(i), p2);
	for(int j=p2.rows.size()-1;j>=0&&p2.rows.size()>0;j--) {
		if(x.compareTo((Comparable) p2.rows.get(j).get(key))==0) {
		if(checkHahtable(keys,htblColNameValue,p2.rows.get(j))) {
			
			Set<String> keys2 = bptree.keySet();
			for(String key1: keys2){
				BPTree b3=null;
				b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
				b3.delete(strTableName+key1, (Comparable) p2.rows.get(j).get(key1), new Ref(i, j));
				DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
			}
			Set<String> keys2r = rtree.keySet();
			for(String key1: keys2r){
				RTree b3=null;
				b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
				b3.delete(strTableName+key1, (Comparable) p2.rows.get(j).get(key1), new Ref(i, j));
				DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
			}
			//System.out.println(p2.rows.size());
			p2.rows.remove(j);
			if(j<p2.rows.size()) {
				Set<String> keys3 = bptree.keySet();
				for(String key1: keys3){
					BPTree b3=null;
					b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");
					b3.shiftInPageForDelete(strTableName+key1,i,j);
					DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
				}
				Set<String> keys3r = rtree.keySet();
				for(String key1: keys3r){
					RTree b3=null;
					b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");
					b3.shiftInPageForDelete(strTableName+key1,i,j);
					DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
				}}
			if (p2.rows.size()==0){
				Set<String> keys7 = bptree.keySet();
			for(String key1: keys7){
				BPTree b3=null;
				b3=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b3);
				DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");
				b3.shiftPagesForDelete(strTableName+key1,i);
				DBApp.serialize(b3,"data/"+bptree.get(key1)+".class");	
			}
			Set<String> keys7r = rtree.keySet();
			for(String key1: keys7r){
			RTree b3=null;
				b3=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b3);
				DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");
				b3.shiftPagesForDelete(strTableName+key1,i);
				DBApp.serialize(b3,"data/"+rtree.get(key1)+".class");	
			}}
			//System.out.println(p2.rows.size());
			
			//current=p2;
			if(p2.rows.size()==0) {

				for(int r=i;r<pages.size()-1;r++) {
					String ss=pages.get(r);
					File f=new File(ss);
					File d=new File(pages.get(r+1));
					d.renameTo(f);
				}
				File f=new File(pages.get(pages.size()-1));
				f.delete();
				pages.remove(pages.size()-1);
				break;
			}
	}}else 
	{
				break;}
}if(p2.rows.size()>0)
	DBApp.serialize(p2, pages.get(i));
	//DBApp.serialize(p2, name+i);

}
}}}
	public boolean checkHahtable(Set<String> keys,Hashtable<String,Object> h1,Hashtable<String,Object> h2) {
		for(String key: keys){
			//System.out.println(key);
			if(!(((Comparable) h1.get(key)).compareTo((Comparable)h2.get(key))==0)) {
				return false;
			}
			//System.out.println(h1.get(key).getClass().toString());
			if(h1.get(key).getClass().toString().equals("class T_221B.polygon"))
				if(!((polygon)(h1).get(key)).toString().equals(((polygon)h2.get(key)).toString()))
					return false;
		}
		return true;
	}
	public boolean checkHahtable4(Set<String> keys,Hashtable<String,Object> h1,Hashtable<String,Object> h2) {
		for(String key: keys){
			Comparable c1=(Comparable) h1.get(key);
			Comparable c2=(Comparable) h2.get(key);
			if(!(c1.compareTo(c2)==0)) {
				return false;
			}
		}
		return true;
	}
	public Table(String strTableName,String strClusteringKeyColumn,Hashtable<String,String> htblColNameType) throws IOException {
		this.strTableName=strTableName;
		this.strClusteringKeyColumn=strClusteringKeyColumn;
		this.htblColNameType=htblColNameType;
		pages=new Vector<String>(1,1);
		maxSize=getPropValues();
		
	}
	public void insertIntoTable(Hashtable<String,Object> htblColNameValue,String key,String name) throws DBAppException, IOException {
		int lastPage;
		int insertionIndex;
		int insertionpage;
		if(pages.size()==0) {
			//System.out.println("in");
			page p=new page(maxSize);
			//System.out.println(key);
			p.insertIntoPage("data/"+name+0,htblColNameValue, key);
			DBApp.serialize(p, "data/"+name+0+".class");
			pages.add("data/"+name+0+".class");
			//add in bPlus trees
			Set<String> keys = bptree.keySet();
			for(String key1: keys){
				BPTree b=null;
				b=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b);
				b.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(0,0));
				DBApp.serialize(b,"data/"+bptree.get(key1)+".class");	
			}	
			Set<String> keysr = rtree.keySet();
			for(String key1: keysr){
				RTree b=null;
				b=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b);
				b.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(0,0));
				DBApp.serialize(b,"data/"+rtree.get(key1)+".class");	
			}	
		}
		else {
			//if key has Bplustree index
			if(bptree.containsKey(key)) {
				insertBplus(htblColNameValue,key, name);
			}
			//if key has Rtree index
			else if(rtree.containsKey(key)) {
				insertR(htblColNameValue,key, name);
			}
			else {
				//System.out.println("in2");
	int  upperbound=pages.size()-1;
		int lowerbound=0;
		int middlePage=0;
		while(lowerbound < upperbound)
        {
            middlePage = (lowerbound + upperbound) / 2;
            page m=null;
			m=(page) DBApp.deserialize(pages.get(middlePage), m);
     if ((m).lessThan( htblColNameValue,key))
                upperbound = middlePage ;
            else
                lowerbound = middlePage + 1;
        }
       int use=upperbound;
       insertionpage=use;
       lastPage=use;
       page usePage=null;
		usePage=(page) DBApp.deserialize(pages.get(use), usePage);
		if(usePage.rows.size()<usePage.maxSize) {
			 insertionIndex=usePage.insertIntoPage(pages.get(use),htblColNameValue, key);
			 DBApp.serialize(usePage, pages.get(use));
		}
		else {
			if(use==pages.size()-1) {
				page last=new page(maxSize);
				String string="data/"+name+pages.size()+".class";
				DBApp.serialize(last,string );
				pages.add(string);
			}
			 insertionIndex= usePage.insertIntoPage(pages.get(use),htblColNameValue, key);
		       page previous=usePage;
       for(int k=use+1;k<pages.size();k++) {
    	   page current=null;
			current=(page) DBApp.deserialize(pages.get(k), current);
    	   current.rows.add(0,previous.rows.get(previous.rows.size()-1));
    	   previous.rows.remove(previous.rows.size()-1);
    	   DBApp.serialize(previous, pages.get(k-1));
    	   previous=current;
    	   if(current.rows.size()<=current.maxSize) {
    		   lastPage=k;
    		   DBApp.serialize(current, pages.get(k));
    		   break;
    	   }
    	   else if(k==pages.size()-1) {
    		   page last=new page(maxSize);
				String string="data/"+name+pages.size()+".class";
				DBApp.serialize(last,string );
				pages.add(string);
    	   }
    	

	}   //insert in bPlus tree
	  
		}
		Set<String> keys = bptree.keySet();
		for(String key1: keys){
			BPTree b=null;
			b=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b);
			b.shiftForInsert(strTableName+key1,insertionpage,insertionIndex, lastPage,maxSize);
		
			b.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(insertionpage, insertionIndex));
			DBApp.serialize(b,"data/"+bptree.get(key1)+".class");	
   }
		Set<String> keysr = rtree.keySet();
		for(String key1: keysr){
			RTree b=null;
			b=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b);
			b.shiftForInsert(strTableName+key1,insertionpage,insertionIndex, lastPage,maxSize);

			b.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(insertionpage, insertionIndex));
			DBApp.serialize(b,"data/"+rtree.get(key1)+".class");	
   }
		}}}
	private void insertR(Hashtable<String, Object> htblColNameValue, String key, String name) throws DBAppException, IOException {
	
		int lastPage;
		int insertionIndex;
		int insertionpage;
		RTree b=null;
		b=(RTree) DBApp.deserialize("data/"+rtree.get(key)+".class", b);
		Ref r=b.getIndexForInsert((Comparable)htblColNameValue.get(key),maxSize);
		insertionpage=r.getPage();
		lastPage=insertionpage;
		insertionIndex=r.getIndexInPage();
		//System.out.println("index:"+insertionpage);
		//System.out.println("index:"+insertionIndex);
		  if(r.getPage()==pages.size()) {
			  page last=new page(maxSize);
				String string="data/"+name+pages.size()+".class";
				DBApp.serialize(last,string );
				pages.add(string);
			  
		  }
		 page usePage2=null;
			usePage2=(page) DBApp.deserialize(pages.get(insertionpage), usePage2);
		if(usePage2.rows.size()<usePage2.maxSize) {
			
			 usePage2.rows.add(insertionIndex, htblColNameValue);
			 DBApp.serialize(usePage2, pages.get(r.getPage()));
				
		}
		else {
		//System.out.println("in2");
			if(r.getPage()==pages.size()-1) {
				page last=new page(maxSize);
				String string="data/"+name+pages.size()+".class";
				DBApp.serialize(last,string );
				pages.add(string);
			}
			
			usePage2=(page) DBApp.deserialize(pages.get(insertionpage), usePage2);
		       usePage2.rows.add(insertionIndex, htblColNameValue);
		       DBApp.serialize(usePage2, pages.get(r.getPage()));
       for(int k=r.getPage()+1;k<pages.size();k++) {
    	   page current=null;
			current=(page) DBApp.deserialize(pages.get(k), current);
			 page previous=null;
				previous=(page) DBApp.deserialize(pages.get(k-1), previous);
				previous.rows.size();
    	   current.insertIntoPage(pages.get(k),previous.rows.get(previous.rows.size()-1),key);
    	   previous.rows.remove(previous.rows.size()-1);
    	   DBApp.serialize(current, pages.get(k));
    	   DBApp.serialize(previous, pages.get(k-1));
    	   if(current.rows.size()<=current.maxSize) {
    		   lastPage=k;
    		   break;
    	   }
    	   
    	   else if(k==pages.size()-1) {
    		   page last=new page(maxSize);
				String string="data/"+name+pages.size()+".class";
				DBApp.serialize(last,string );
				pages.add(string);
    	   }
       }
       
 //System.out.println("last"+lastPage);
		
	}  
		b.shiftForInsert(strTableName+key,insertionpage,insertionIndex, lastPage,maxSize);
		b.insert(strTableName+key, (Comparable)htblColNameValue.get(key), new Ref(insertionpage, insertionIndex));
		DBApp.serialize(b,"data/"+rtree.get(key)+".class");	
	Set<String> keys = bptree.keySet();
	for(String key1: keys){
		//System.out.println("did2");
		BPTree b2=null;
		b2=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b2);
		b2.shiftForInsert(strTableName+key1,insertionpage,insertionIndex, lastPage,maxSize);
		b2.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(insertionpage, insertionIndex));
		DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");	
	}
	Set<String> keysr = rtree.keySet();
	for(String key1: keysr){
		if(!(key1.equals(key))) {
		//System.out.println("did2");
		RTree b2=null;
		b2=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b2);
		b2.shiftForInsert(strTableName+key1,insertionpage,insertionIndex, lastPage,maxSize);
		b2.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(insertionpage, insertionIndex));
		DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");	
	}}
	}
		
	private void insertBplus(Hashtable<String, Object> htblColNameValue, String key, String name) throws IOException, DBAppException {
		//System.out.println("wooooooooo");
		int lastPage;
		int insertionIndex;
		int insertionpage;
		BPTree b=null;
		b=(BPTree) DBApp.deserialize("data/"+bptree.get(key)+".class", b);
		Ref r=b.getIndexForInsert((Comparable)htblColNameValue.get(key),maxSize);
		insertionpage=r.getPage();
		lastPage=insertionpage;
		insertionIndex=r.getIndexInPage();
		//System.out.println("index:"+insertionpage);
		//System.out.println("index:"+insertionIndex);
		  if(r.getPage()==pages.size()) {
			  page last=new page(maxSize);
				String string="data/"+name+pages.size()+".class";
				DBApp.serialize(last,string );
				pages.add(string);
			  
		  }
		 page usePage2=null;
			usePage2=(page) DBApp.deserialize(pages.get(insertionpage), usePage2);
		if(usePage2.rows.size()<usePage2.maxSize) {
			//System.out.println("in1");
			 usePage2.rows.add(insertionIndex, htblColNameValue);
			 DBApp.serialize(usePage2, pages.get(r.getPage()));
				
		}
		else {
		//System.out.println("in2");
			if(r.getPage()==pages.size()-1) {
				page last=new page(maxSize);
				String string="data/"+name+pages.size()+".class";
				DBApp.serialize(last,string );
				pages.add(string);
			}
			
			usePage2=(page) DBApp.deserialize(pages.get(insertionpage), usePage2);
		       usePage2.rows.add(insertionIndex, htblColNameValue);
		       page previous=usePage2;
		       for(int k=insertionpage+1;k<pages.size();k++) {
		    	   page current=null;
					current=(page) DBApp.deserialize(pages.get(k), current);
		    	   current.rows.add(0,previous.rows.get(previous.rows.size()-1));
		    	   previous.rows.remove(previous.rows.size()-1);
		    	   DBApp.serialize(previous, pages.get(k-1));
		    	   previous=current;
		    	   if(current.rows.size()<=current.maxSize) {
		    		   lastPage=k;
		    		   DBApp.serialize(current, pages.get(k));
		    		   break;
		    	   }
		    	   else if(k==pages.size()-1) {
		    		   page last=new page(maxSize);
						String string="data/"+name+pages.size()+".class";
						DBApp.serialize(last,string );
						pages.add(string);
		    	   }
		    	

			} 
       
 //System.out.println("last"+lastPage);
		
	}  
	
		b.shiftForInsert(strTableName+key,insertionpage,insertionIndex, lastPage,maxSize);
		b.insert(strTableName+key, (Comparable)htblColNameValue.get(key), new Ref(insertionpage, insertionIndex));
		DBApp.serialize(b,"data/"+bptree.get(key)+".class");	
	Set<String> keys = bptree.keySet();
	for(String key1: keys){
		if(!key1.equals(key)) {
		//System.out.println("did2");
		BPTree b2=null;
		b2=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b2);
		b2.shiftForInsert(strTableName+key1,insertionpage,insertionIndex, lastPage,maxSize);
		b2.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(insertionpage, insertionIndex));
		DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");	
	}}
	Set<String> keysr = rtree.keySet();
	for(String key1: keysr){
		//System.out.println("did2");
		RTree b2=null;
		b2=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b2);
		b2.shiftForInsert(strTableName+key1,insertionpage,insertionIndex, lastPage,maxSize);
		
		b2.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(insertionpage, insertionIndex));
		DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");	
	}
	}
	public void updateBPlusWithkey(Hashtable<String, Object> htblColNameValue, String strTableName2, String key,
			String keyType, String strKey) throws IOException {
		//System.out.println(keyType);
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
		Set<String> keys =  htblColNameValue.keySet();
		BPTree b=null;
		b=(BPTree) DBApp.deserialize("data/"+bptree.get(key)+".class", b);
		//changeed
		String v=b.search(keyValue);
		Ref2 r2=null;
		//System.out.println(v);
		if(!(v==null)) {
		r2=(Ref2) DBApp.deserialize(v, r2);
		int i=0;
		while(i<r2.ref.size()) {
			page p2=null;
			p2=(page) DBApp.deserialize(pages.get(r2.ref.get(i).getPage()), p2);
			int current=r2.ref.get(i).getPage();
			while(i<r2.ref.size()&& r2.ref.get(i).getPage()==current) {
				for(String key1: keys){	
				if(bptree.containsKey(key1)) {
		
					BPTree b2=null;
					b2=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b2);
					b2.delete(strTableName2+key1,(Comparable) p2.rows.get(r2.ref.get(i).getIndexInPage()).get(key1), new Ref(r2.ref.get(i).getPage(),r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");
					//System.out.println(htblColNameValue.get(key1));
					b2.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(r2.ref.get(i).getPage(),r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");	
				}
				if(rtree.containsKey(key1)) {
					RTree b2=null;
					b2=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b2);
					b2.delete(strTableName2+key1,(Comparable) p2.rows.get(r2.ref.get(i).getIndexInPage()).get(key1), new Ref(r2.ref.get(i).getPage(),r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");
					//System.out.println(htblColNameValue.get(key1));
					b2.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(r2.ref.get(i).getPage(),r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");	
				}
				LocalDateTime l=DBApp.CurrentDate();
				java.util.Date d=new Date(); 
				p2.rows.get(r2.ref.get(i).getIndexInPage()).put("TouchDate",d);
				p2.rows.get(r2.ref.get(i).getIndexInPage()).put(key1,htblColNameValue.get(key1));
				}
				//System.out.println("out");
				i++;
			}
			DBApp.serialize(p2,pages.get(current));
		}
	}
	}
	public void updateRWithkey(Hashtable<String, Object> htblColNameValue, String strTableName2, String key,
			String keyType, String strKey) throws IOException {
//		System.out.println("herej");
		Comparable keyValue = null;
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
	    	 int h=i*2;
	    	 x[i]=Integer.parseInt(s2.get(h));
	    	 y[i]=Integer.parseInt(s2.get(h+1));
	     }
	     polygon po=new polygon(x,y,s2.size()/2);
//	     for(int i=0;i<po.xpoints.length;i++) {
//	    	 System.out.println("x"+po.xpoints[i]);
//	    	 System.out.println("y"+po.ypoints[i]);
//	     }
	     keyValue=po;
		Set<String> keys =  htblColNameValue.keySet();
		RTree b=null;
		b=(RTree) DBApp.deserialize("data/"+rtree.get(key)+".class", b);
		String v=b.search(keyValue);
		Ref2 r2=null;
		//System.out.println(v);
		if(!(v==null)) {
		r2=(Ref2) DBApp.deserialize(v, r2);
		//System.out.println(r2==null);
		//System.out.println("size"+r2.ref.size());
		int i=0;
		while(i<r2.ref.size()) {
			page p2=null;
			p2=(page) DBApp.deserialize(pages.get(r2.ref.get(i).getPage()), p2);
			int current=r2.ref.get(i).getPage();
			while(i<r2.ref.size()&& r2.ref.get(i).getPage()==current) {
				for(String key1: keys){	
					if(p2.rows.get(r2.ref.get(i).getIndexInPage()).get(key).toString().equals(keyValue.toString())) {
				if(bptree.containsKey(key1)) {
		
					BPTree b2=null;
					b2=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b2);
					b2.delete(strTableName2+key1,(Comparable) p2.rows.get(r2.ref.get(i).getIndexInPage()).get(key1), new Ref(r2.ref.get(i).getPage(),r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");
					//System.out.println(htblColNameValue.get(key1));
					b2.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(r2.ref.get(i).getPage(),r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");	
				}
				if(rtree.containsKey(key1)) {
					RTree b2=null;
					b2=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b2);
					b2.delete(strTableName2+key1,(Comparable) p2.rows.get(r2.ref.get(i).getIndexInPage()).get(key1), new Ref(r2.ref.get(i).getPage(),r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");
					//System.out.println(htblColNameValue.get(key1));
					
					b2.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(r2.ref.get(i).getPage(),r2.ref.get(i).getIndexInPage()));
					DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");	
				}
				
				java.util.Date d=new Date(); 
				p2.rows.get(r2.ref.get(i).getIndexInPage()).put("TouchDate",d);
				p2.rows.get(r2.ref.get(i).getIndexInPage()).put(key1,htblColNameValue.get(key1));
				}}
				//System.out.println("out");
				i++;
			}
			DBApp.serialize(p2,pages.get(current));
		}
	}
	}
	public void updateTabe(String strTableName2, Comparable keyValue, String Key, Hashtable<String, Object> htblColNameValue) throws IOException {
		boolean continu=true;

		if(pages.size()>0) {
		
			Set<String> keys = htblColNameValue.keySet();
			Comparable<Comparable> x = keyValue;
		
			int  upperbound=pages.size()-1;
			int lowerbound=0;
			int middlePage=0;
			while(lowerbound < upperbound)
		    {
		middlePage = (lowerbound + upperbound) / 2;
		//System.out.println(middlePage);
		        page m=null;
				m=(page) DBApp.deserialize(pages.get(middlePage), m);
				//System.out.println((m).lessThan( htblColNameValue,key));
		 if (((Comparable) m.rows.get(m.rows.size()-1).get(Key)).compareTo(keyValue)>0)
		            upperbound = middlePage ;
		        else
		            lowerbound = middlePage + 1;
		    }
		   int use=upperbound;
		   
		  // System.out.println("index"+use);
		   //System.out.println(use);
		   page usePage=null;
			usePage=(page) DBApp.deserialize(pages.get(use), usePage);
			boolean b= usePage.updatePage(htblColNameValue,keyValue,Key,bptree,rtree,use,strTableName2);
//			System.out.println("index"+b);
//			System.out.println("seriaized");
			DBApp.serialize(usePage, pages.get(use));
			if(b==true) {
		for(int i= use-1;i>=0;i--) {
			 page p=null;
				p=(page) DBApp.deserialize(pages.get(i), p);
				for(int j=p.rows.size()-1;j>=0;j--) {
					if(((Comparable) p.rows.get(j).get(Key)).compareTo(keyValue)==0) {
						if(keyValue.getClass().toString().equals("class T_221B.polygon")) {
							if(((polygon)keyValue).toString().equals(((polygon)p.rows.get(j).get(Key)).toString())) {
						Set Keys=htblColNameValue.keySet();
						for(String key1:keys) {
							if(bptree.containsKey(key1)) {
								
								BPTree b2=null;
								b2=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b2);
								b2.delete(strTableName2+key1,(Comparable) p.rows.get(j).get(key1), new Ref(i,j));
								DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");
								//System.out.println(htblColNameValue.get(key1));
								b2.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(i,j));
								DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");	
							}
							if(rtree.containsKey(key1)) {
								RTree b2=null;
								b2=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b2);
								b2.delete(strTableName2+key1,(Comparable) p.rows.get(j).get(key1), new Ref(i,j));
								DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");
								//System.out.println(htblColNameValue.get(key1));
								b2.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(i,j));
								DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");	
							}
							
							p.rows.get(j).put(key1, htblColNameValue.get(key1));
							
							 
						}
						LocalDateTime l=DBApp.CurrentDate();
						java.util.Date d=new Date(); 
						p.rows.get(j).put("TouchDate",d);
						
					}}else{Set Keys=htblColNameValue.keySet();
					for(String key1:keys) {
						if(bptree.containsKey(key1)) {
							
							BPTree b2=null;
							b2=(BPTree) DBApp.deserialize("data/"+bptree.get(key1)+".class", b2);
							b2.delete(strTableName2+key1,(Comparable) p.rows.get(j).get(key1), new Ref(i,j));
							DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");
							//System.out.println(htblColNameValue.get(key1));
							b2.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(i,j));
							DBApp.serialize(b2,"data/"+bptree.get(key1)+".class");	
						}
						if(rtree.containsKey(key1)) {
							RTree b2=null;
							b2=(RTree) DBApp.deserialize("data/"+rtree.get(key1)+".class", b2);
							b2.delete(strTableName2+key1,(Comparable) p.rows.get(j).get(key1), new Ref(i,j));
							DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");
							//System.out.println(htblColNameValue.get(key1));
							b2.insert(strTableName+key1, (Comparable)htblColNameValue.get(key1), new Ref(i,j));
							DBApp.serialize(b2,"data/"+rtree.get(key1)+".class");	
						}
						
						p.rows.get(j).put(key1, htblColNameValue.get(key1));
						
						 
					}
					LocalDateTime l=DBApp.CurrentDate();
					java.util.Date d=new Date(); 
					p.rows.get(j).put("TouchDate",d);
					
						}
					}
					else {
							continu=false;
							break;}
				}
				
				DBApp.serialize(p, pages.get(i));
				if(continu==false)
					break;
		}
	
			}}}
	public void searchInTableBinaryLess(Vector<Hashtable<String, Object>> search, String columnName, Object value) {
		if(pages.size()>0) {
			Comparable x = (Comparable)value;
			int  upperbound=pages.size()-1;
			int lowerbound=0;
			int middlePage=0;
			boolean still=false;
			boolean found=false;
			boolean keyFound=false;
			while(lowerbound < upperbound)
		    {
		middlePage = (lowerbound + upperbound) / 2;
		        page m=null;
				m=(page) DBApp.deserialize(pages.get(middlePage), m);
		 if ((m).lessThan3( x,columnName))
		            upperbound = middlePage ;
		        else
		            lowerbound = middlePage + 1;
		    }
			
		   int use=upperbound;
		   //System.out.println(use);
		   page usePage=null;
			usePage=(page) DBApp.deserialize(pages.get(use), usePage);
			 usePage.searchInPageLess( search,x,columnName);
			 int y=search.size();
		if(use>0) { 
			//System.out.println("in");
			for(int i=0;i<use;i++) {
			page p2=null;		
			p2=(page) DBApp.deserialize(pages.get(i), p2);
			for(int j=0;j<p2.rows.size();j++) {
				if(x.compareTo((Comparable) p2.rows.get(j).get(columnName))>0) {
					search.add(search.size()-y,p2.rows.get(j));}
			}
		//System.out.println(search.size());
		}
		}}
		
		
	}
	public void searchInTableBinaryLessOrEqual(Vector<Hashtable<String, Object>> search, String columnName,
			Object value) {
		Comparable x = (Comparable)value;
		int  upperbound=pages.size()-1;
		int lowerbound=0;
		int middlePage=0;
		boolean still=false;
		boolean found=false;
		boolean keyFound=false;
		while(lowerbound < upperbound)
	    {
	middlePage = (lowerbound + upperbound) / 2;
	        page m=null;
			m=(page) DBApp.deserialize(pages.get(middlePage), m);
	 if ((m).lessThan3( x,columnName))
	            upperbound = middlePage ;
	        else
	            lowerbound = middlePage + 1;
	    }
		
	   int use=upperbound;
	   //System.out.println(use);
	   page usePage=null;
		usePage=(page) DBApp.deserialize(pages.get(use), usePage);
		 usePage.searchInPageLessOrEqual( search,x,columnName);
		 int y=search.size();
	if(use>0) { 
		//System.out.println("in");
		for(int i=0;i<use;i++) {
		page p2=null;		
		p2=(page) DBApp.deserialize(pages.get(i), p2);
		for(int j=0;j<p2.rows.size();j++) {
			if(x.compareTo((Comparable) p2.rows.get(j).get(columnName))>=0) {
				search.add(search.size()-y,p2.rows.get(j));}
		}
	System.out.println(search.size());
	}
	}}
	public void searchInTableBinaryMore(Vector<Hashtable<String, Object>> search, String columnName, Object value) {
		if(pages.size()>0) {
			Comparable x = (Comparable)value;
			int  upperbound=pages.size()-1;
			int lowerbound=0;
			int middlePage=0;
			boolean still=false;
			boolean found=false;
			boolean keyFound=false;
			while(lowerbound < upperbound)
		    {
		middlePage = (lowerbound + upperbound) / 2;
		        page m=null;
				m=(page) DBApp.deserialize(pages.get(middlePage), m);
		 if ((m).lessThan3( x,columnName))
		            upperbound = middlePage ;
		        else
		            lowerbound = middlePage + 1;
		    }
			
		   int use=upperbound;
		   //System.out.println(use);
		   page usePage=null;
			usePage=(page) DBApp.deserialize(pages.get(use), usePage);
			 usePage.searchMore( search,x,columnName);
		if(use<pages.size()) { 
			//System.out.println("in");
			for(int i=use+1;i<pages.size();i++) {
			page p2=null;		
			p2=(page) DBApp.deserialize(pages.get(i), p2);
			for(int j=0;j<p2.rows.size();j++) {
				if(x.compareTo((Comparable) p2.rows.get(j).get(columnName))<0) {
					search.add(p2.rows.get(j));}
			}
		//System.out.println(search.size());
		}
		}}
		
		}
	
	public void searchInTableBinaryMoreOrEqual(Vector<Hashtable<String, Object>> search, String columnName,
			Object value) {
		if(pages.size()>0) {
			Comparable x = (Comparable)value;
			int  upperbound=pages.size()-1;
			int lowerbound=0;
			int middlePage=0;
			boolean still=false;
			boolean found=false;
			boolean keyFound=false;
			while(lowerbound < upperbound)
		    {
		middlePage = (lowerbound + upperbound) / 2;
		//System.out.println(middlePage);
		        page m=null;
				m=(page) DBApp.deserialize(pages.get(middlePage), m);
				//System.out.println((m).lessThan( htblColNameValue,key));
		 if ((m).lessThan3( x,columnName))
		            upperbound = middlePage ;
		        else
		            lowerbound = middlePage + 1;
		    }
			
		   int use=upperbound;
		   page usePage=null;
			usePage=(page) DBApp.deserialize(pages.get(use), usePage);
			boolean[] b= usePage.searchMoreOrEqual( search,x,columnName);
			found=b[0];
			still=b[1];
			keyFound=b[2];
			//System.out.println(still);
			//System.out.println(keyFound);
		if(use>0 && (still==true||keyFound==false)) { 
			for(int i=use-1;i>=0;i--) {
			page p2=null;		
			p2=(page) DBApp.deserialize(pages.get(i), p2);
			for(int j=p2.rows.size()-1;j>=0&&p2.rows.size()>0;j--) {
				if(x.compareTo((Comparable) p2.rows.get(j).get(columnName))==0) {
					
					search.add(p2.rows.get(j));}
					else 
					{break;}	
			}
		
		}
		}
		if(use<pages.size()) { 
			//System.out.println("in");
			for(int i=use+1;i<pages.size();i++) {
			page p2=null;		
			p2=(page) DBApp.deserialize(pages.get(i), p2);
			for(int j=0;j<p2.rows.size();j++) {
				if(x.compareTo((Comparable) p2.rows.get(j).get(columnName))<=0) {
					search.add(p2.rows.get(j));}
			}
	}}
	
}

	}}

	
	
    
	