package com.sinward.rec_sys.OfflineCalculater;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

public class DocVec2File {
	public static void main(String args[]) throws Exception{
		docVec2File("D:/download/docVecFile");
	}
	
	public static void docVec2File(String path) throws Exception{
		//从数据库中获取DocVec
		List<Tuple2<Integer,double[]>> result=new ArrayList<Tuple2<Integer,double[]>>(Config.DOC_NUM);
		String url = Config.DB.JDBC_URL;
		String user = Config.DB.USER;
		String password = Config.DB.PASSWORD;
		int count=0;
		FileWriter fw=new FileWriter(path,false);
		
		Class.forName("org.postgresql.Driver");
		
		Connection conn = DriverManager.getConnection(url, user, password);
		Statement state = conn.createStatement();
		int offset=0,fetch_size=100000;
		while(true) {
			ResultSet rs = state.executeQuery("SELECT docid,vec FROM doc_vec LIMIT "+fetch_size
					+" OFFSET "+offset);
			offset+=fetch_size;
			if(rs.next()) {
				do {
					int docid=rs.getInt(1);
					String buf=""+docid;
					Double[] data=(Double[])rs.getArray(2).getArray();
					for(int i=0;i<data.length;i++) buf+=" "+data[i];
					fw.append(buf);
					fw.append("\n");
					count++;
				}while(rs.next());
			}else break;
			System.out.println("count="+count+" percentage="+((double)count/Config.DOC_NUM)*100+"%");
		}
		System.out.println("count="+count);
		fw.close();
	}
}
