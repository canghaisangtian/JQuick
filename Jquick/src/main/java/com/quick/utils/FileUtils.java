package com.quick.utils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Value;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;


public class FileUtils {
	
	

	public static void main(String[] args) throws Exception {
		  String fileName="H:\\";
	        File f=new File(fileName);
	        List<String> list=new ArrayList<String>();
		     listFile( f,list);
		     for (String string : list) {
				System.out.println(string);
				read(string);
			}
//		read();
	}
	 public static void listFile(File f,List<String>  list){
		
	        if(f!=null){
	            if(f.isDirectory()){
	                File[] fileArray=f.listFiles();
	                if(fileArray!=null){
	                    for (int i = 0; i < fileArray.length; i++) {
	                        //递归调用
	                    	listFile(fileArray[i],list);
	                    }
	                }
	            }
	            else{
	            	 list.add(f.toString());
//	                System.out.println(f);
	            }
	        }
	    }

	  @Test
	   public void write() throws Exception
	   {
		
	      Appendable out = new PrintWriter("file.csv");
	      CSVPrinter printer = CSVFormat.DEFAULT.withHeader("userId", "userName")
	            .print(out);
	      for (int i = 0; i < 10; i++)
	      {
	         printer.printRecord("userId" + i, "userName" + i);
	      }
	      printer.flush();
	      printer.close();
	   }

	   @Test
	   public static void read(String fileName) throws Exception
	   {
		   
		  List<Document> list=new ArrayList<Document>();
	      InputStreamReader reader= new InputStreamReader(new FileInputStream(fileName), "GBK");
	      CSVParser parser = CSVFormat.DEFAULT.withHeader()
	            .parse(reader);
	      for (CSVRecord record : parser)
	      {
	   
	      }
	      reader.close();
	      
	      MongodbUtils.insertList(list, "");
	   }

	   
	
}
