package com.sinward.rec_sys.OfflineCalculater;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.MapPartitionsFunction;

import org.apache.commons.collections.IteratorUtils;

import java.util.*;
import java.io.FileInputStream;
import java.lang.Math;
import java.sql.*;

import scala.Tuple2;
import scala.collection.Seq;

import com.sinward.rec_sys.OfflineCalculater.SimItem;

/*class IsDown implements Function<Tuple2<Row,Row>,Boolean>{

	public Boolean call(Tuple2<Row, Row> v) throws Exception {
		// TODO 判断某一个Tuple2<Row,Row>是否在相似度矩阵的左下半部分
		return v._1().<Integer>getAs("docid")>v._2().<Integer>getAs("docid");
	}
}

class GetSim implements Function<Tuple2<Row,Row>,SimItem>{

	public SimItem call(Tuple2<Row, Row> v) throws Exception {
		// TODO 将一个Row,Row的元组转换成为一项相似度的数据项
		List<Double> v1=null,v2 = null;
		int i;
		double dot=0,len1=0,len2=0,x1,x2;//分别是v1v2的点积，v1的长度，v2的长度
		String url1,url2;
		int docid1,docid2;
		
		//System.out.println("getting similarity...");
		
		url1=v._1().<String>getAs("url");
		url2=v._2().<String>getAs("url");
		
		docid1=v._1().<Integer>getAs("docid");
		docid2=v._2().<Integer>getAs("docid");
		
		
		v1=v._1().getList(1);
		v2=v._2().getList(1);
		for(i=0;i<v1.size();i++) {
			x1=v1.get(i);
			x2=v2.get(i);
			dot+=x1*x2;
			len1+=x1*x1;
			len2+=x2*x2;
		}
		len1=Math.sqrt(len1);
		len2=Math.sqrt(len2);
		
		SimItem result=new SimItem();
		result.setUrl1(url1);
		result.setUrl2(url2);
		result.setDocid1(docid1);
		result.setDocid2(docid2);
		result.setSim(dot/(len1*len2));
		
		return result;
	}
	
}

class GetKSim implements FlatMapFunction<Tuple2<String,Iterable<SimItem>>,SimItem>{
	private int k;
	GetKSim(int _k){k=_k;}
	public Iterator<SimItem> call(Tuple2<String,Iterable<SimItem>> kv) throws Exception {
		// TODO 将input中的所有sim，计算最高的k项，并返回
		PriorityQueue<SimItem> pq=new PriorityQueue<SimItem>(k+1);
		int i;
		
		//System.out.println("getting K similarity...");
		Iterator<SimItem> input=kv._2().iterator();
		for(i=0;i<k;i++) {
			if(input.hasNext()) pq.add(input.next());
		}
		while(input.hasNext()) {
			pq.add(input.next());
			pq.remove();
		}
		List<SimItem> result=new ArrayList<SimItem>(k);
		while(!pq.isEmpty()) result.add(pq.remove());
		return result.iterator();
	}
	
}

class Sim2Pair implements PairFunction<SimItem,String,SimItem>{

	public Tuple2<String, SimItem> call(SimItem v) throws Exception {
		// TODO 将SimItem映射成为(url1,SimItem)
		return new Tuple2<String,SimItem>(v.getUrl1(),v);
	}
	
}

class Pair2Sim implements Function<Tuple2<String,Iterator<SimItem>>,Iterator<SimItem>>{

	public Iterator<SimItem> call(Tuple2<String, Iterator<SimItem>> v) throws Exception {
		// TODO 将(url1,SimItem...)映射成为SimItem...
		return v._2();
	}
	
}*/

class GetKSim2 implements MapPartitionsFunction<Row,SimItem>{
	private int k;	    
    Broadcast<List<double[]>> bcVecList;
	
	
	public GetKSim2(int k,Broadcast<List<double[]>> bcVecList){
		this.k=k;
		this.bcVecList=bcVecList;
	}
	
	public Iterator<SimItem> call(Iterator<Row> input) throws Exception {
		// TODO 将初始的Row映射成为最大的K个SimItem
		System.out.println("start map partitions");
		List<SimItem> result=new ArrayList<SimItem>();
		PriorityQueue<SimItem> pq=new PriorityQueue<SimItem>(k+1);
		double cos=0;
		int docid1,docid2,i;
		double[] vec2;
		List<double[]> vecList=bcVecList.value();
		Set<Integer> calculated=new HashSet<Integer>(vecList.size());//记录已经计算过的docid2
		
		while(input.hasNext()) {
			Row row=input.next();
			docid1=row.<Integer>getAs("docid");
			List<Double> vec1=row.getList(1);
			calculated.clear();
			for(i=0;i<vecList.size();i++){
				vec2=vecList.get(i);
				docid2=(int)vec2[Config.WORD_VEC_DIM];
				if(docid1==docid2||calculated.contains(docid2)) continue;
				calculated.add(docid2);
				cos=getCos(vec1,vec2);
				SimItem sim=new SimItem();
				sim.setDocid1(docid1);
				sim.setDocid2(docid2);
				sim.setSim(cos);
				pq.add(sim);
				if(pq.size()>k) pq.remove();
			}
			//将堆中的元素推到result中
			while(!pq.isEmpty()) result.add(pq.remove());
		}
		return result.iterator();
	}
	
	public double getCos(List<Double> v1,double[] v2) {
		double len1=0,len2=0,dot=0;
		int i;
		double x1,x2;
		
		for(i=0;i<v1.size();i++) {
			x1=v1.get(i);
			x2=v2[i];
			len1+=x1*x1;
			len2+=x2*x2;
			dot+=x1*x2;
		}
		return dot/(Math.sqrt(len1*len2));
	}
}

public class CalSim {
	//计算文章相似度，并保留和文章最像的K个向量
	public static void main(String args[]) throws Exception{
		SparkSession spark = SparkSession
	  			  .builder()
	  			  .master("spark://master:7077")
	  			  .appName("OfflineCalculater")
	  			  .config("spark.submit.deployMode","cluster")
	  			  .config("spark.driver.host","192.168.59.1")
	  			  .config("spark.driver.maxResultSize","0")
	  			  .config("spark.sql.crossJoin.enabled","true")
	  			  .config("spark.driver.cores","8")
	  			  .config("spark.driver.memory","12g")
	  			  .config("spark.executor.cores","16")
	  			  .config("spark.executor.memory","22g")
	  			  .config("spark.jars",
	  					  "file://D:/eclipse-workspace/OfflineCalculater/target/OfflineCalculater-0.0.1-SNAPSHOT.jar"
	  					  +",file://D:/temp/postgresql-42.2.12.jar")
	  			  .config("spark.driver.supervise","true")
	  			  .getOrCreate();
	    	SparkContext sc=spark.sparkContext();
	    	JavaSparkContext jsc=new JavaSparkContext(sc);
	    	
	    System.out.println("loading data...");
	    Dataset<Row> docVec = spark.read()
				  .format("jdbc")
				  .option("url", Config.DB.JDBC_URL)
				  .option("user", Config.DB.USER)
				  .option("password", Config.DB.PASSWORD)
				  .option("dbtable", "doc_vec")
				  .option("numPartitions",Config.DataSource.NUM_OF_PARTITIONS)
				  .option("partitionColumn", "docid")
				  .option("lowerBound", Config.DataSource.LOWER_BOUND)
				  .option("upperBound",Config.DataSource.UPPER_BOUND)
				  .load();
	    //docVec.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
	    
	    //广播该变量
	    System.out.println("loading doc vec list...");
	    List<double[]> vecList=getDocVecList();
	    System.out.println("broadcasting doc vec list...");
	    Broadcast<List<double[]>> bcVecList=jsc.broadcast(vecList);
	    
	    System.out.println("calculating similarity...");
	    Encoder<SimItem> simEncoder=Encoders.bean(SimItem.class);
	    Dataset<SimItem> sim=docVec.mapPartitions(new GetKSim2(10,bcVecList),simEncoder);
	    
	    //sim.show();
	    System.out.println("writing...");
	    sim.write().format("jdbc").mode(SaveMode.Overwrite)
	  	.option("url",Config.DB.JDBC_URL)
	  	.option("user",Config.DB.USER)
	  	.option("password",Config.DB.PASSWORD)
	  	.option("dbtable", "most_sim")
	  	.save();
	}
	
	public static List<double[]> getDocVecList() throws Exception{
		//从数据库中获取DocVec
		List<double[]> result=new ArrayList<double[]>(Config.DOC_NUM);
		String url = Config.DB.JDBC_URL;
		String user = Config.DB.USER;
		String password = Config.DB.PASSWORD;
		int count=0;
		
		Class.forName("org.postgresql.Driver");
		
		Connection conn = DriverManager.getConnection(url, user, password);
		Statement state = conn.createStatement();
		int offset=0,fetch_size=1000;
		while(true) {
			ResultSet rs = state.executeQuery("SELECT docid,vec FROM doc_vec LIMIT "+fetch_size
					+" OFFSET "+offset);
			offset+=fetch_size;
			if(rs.next()) {
				do {
					int docid=rs.getInt(1);
					Double[] data=(Double[])rs.getArray(2).getArray();
					double[] light_data=new double[Config.WORD_VEC_DIM+1];
					light_data[Config.WORD_VEC_DIM]=docid;
					for(int i=0;i<data.length;i++) light_data[i]=data[i];
					result.add(light_data);
					count++;
					if(count%fetch_size==0) {
						System.out.println("count="+count+" percentage="+((double)count/Config.DOC_NUM)*100);
					}
				}while(rs.next());
			}else break;
		}
		return result;
	}
}
