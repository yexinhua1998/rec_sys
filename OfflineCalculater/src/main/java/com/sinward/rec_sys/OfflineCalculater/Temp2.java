package com.sinward.rec_sys.OfflineCalculater;

import java.util.*;
import java.io.*;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.sql.*;

import scala.Tuple2;

public class Temp2 {
	public static void main(String args[]) {
		PriorityQueue<Integer> pq=new PriorityQueue<Integer>();
		for(int i=0;i<10;i++) pq.add(0);
		while(!pq.isEmpty()) System.out.println(pq.remove());
	}
	
	public static void getvec(String args[]) throws Exception{
		Scanner s=new Scanner(new FileInputStream("D:/download/docVecFile"));
		int count=0;
		int docid;
		int i;
		while(s.hasNext()) {
			docid=s.nextInt();
			System.out.print(docid+" ");
			for(i=0;i<Config.WORD_VEC_DIM;i++) {
				 s.nextDouble();
				 count++;
			}
		}
		System.out.println("count="+count);
	}
	
	public static List<Tuple2<Integer,double[]>> getDocVecList() throws Exception{
		//从数据库中获取DocVec
		List<Tuple2<Integer,double[]>> result=new ArrayList<Tuple2<Integer,double[]>>(Config.DOC_NUM);
		String url = "jdbc:postgresql://192.168.59.134:5432/test";
		String user = "postgres";
		String password = "Ye25554160";
		int count=0;
		
		Class.forName("org.postgresql.Driver");
		
		Connection conn = DriverManager.getConnection(url, user, password);
		Statement state = conn.createStatement();
		int offset=0,fetch_size=10;
		while(true) {
			ResultSet rs = state.executeQuery("SELECT docid,vec FROM doc_vec LIMIT "+fetch_size
					+" OFFSET "+offset);
			offset+=fetch_size;
			if(rs.next()) {
				do {
					int docid=rs.getInt(1);
					Double[] data=(Double[])rs.getArray(2).getArray();
					double[] light_data=new double[data.length];
					for(int i=0;i<data.length;i++) light_data[i]=data[i];
					result.add(new Tuple2<Integer,double[]>(docid,light_data));
					count++;
					if(count%10==0) {
						System.out.println("count="+count+" percentage="+(double)(count/Config.DOC_NUM)*100);
					}
				}while(rs.next());
			}else break;
		}
		return result;
	}
	
	public static void test_spark(String args[]) throws Exception{
		SparkSession spark = SparkSession
	  			  .builder()
	  			  .master("spark://master:7077")
	  			  .appName("test")
	  			  .config("spark.submit.deployMode","cluster")
	  			  .config("spark.driver.host","192.168.59.1")
	  			  .getOrCreate();
    	SparkContext sc=spark.sparkContext();
    	JavaSparkContext jsc=new JavaSparkContext(sc);
    	
    	List<Integer> data=new ArrayList(5);;
    	for(int i=0;i<5;i++) data.add(i);
		Dataset<Integer> ds=spark.createDataset(data, Encoders.INT());
		
		ds.show();
		
		ds.crossJoin(ds).show();
	}
}
